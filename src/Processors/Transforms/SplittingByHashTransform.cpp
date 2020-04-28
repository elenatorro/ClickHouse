#include <Processors/Transforms/SplittingByHashTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct ChunkInfoWithChunks : public ChunkInfo
{
    mutable Chunks chunks;
};

SplittingByHashTransform::SplittingByHashTransform(
    const Block & header, size_t num_outputs_, ColumnNumbers key_columns_)
    : ISimpleTransform(header, {}, false)
    , num_outputs(num_outputs_)
    , key_columns(std::move(key_columns_))
    , hash(0)
{
    setInputNotNeededAfterRead(false);

    if (num_outputs <= 1)
        throw Exception("SplittingByHashTransform expects more than 1 outputs, got " + std::to_string(num_outputs),
                        ErrorCodes::LOGICAL_ERROR);

    if (key_columns.empty())
        throw Exception("SplittingByHashTransform cannot split by empty set of key columns",
                        ErrorCodes::LOGICAL_ERROR);

    for (auto & column : key_columns)
        if (column >= header.columns())
            throw Exception("Invalid column number: " + std::to_string(column) +
                            ". There is only " + std::to_string(header.columns()) + " columns in header",
                            ErrorCodes::LOGICAL_ERROR);
}

ResizeByHashTransform::ResizeByHashTransform(const Block & header, size_t num_outputs)
    : IProcessor(InputPorts(1, Block()), OutputPorts(num_outputs, header))
{
    if (num_outputs <= 1)
        throw Exception("ResizeByHashTransform expects more than 1 outputs, got " + std::to_string(num_outputs),
                        ErrorCodes::LOGICAL_ERROR);
}

static void calculateWeakHash32(const Chunk & chunk, const ColumnNumbers & key_columns, WeakHash32 & hash)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    hash.reset(num_rows);

    for (const auto & column_number : key_columns)
        columns[column_number]->updateWeakHash32(hash);
}

static void fillSelector(const WeakHash32 & hash, size_t num_outputs, IColumn::Selector & selector)
{
    /// Row from interval [(2^32 / num_outputs) * i, (2^32 / num_outputs) * (i + 1)) goes to bucket with number i.

    const auto & hash_data = hash.getData();
    size_t num_rows = hash_data.size();
    selector.resize(num_rows);

    for (size_t row = 0; row < num_rows; ++row)
    {
        selector[row] = hash_data[row]; /// [0, 2^32)
        selector[row] *= num_outputs; /// [0, num_outputs * 2^32), selector stores 64 bit values.
        selector[row] >>= 32u; /// [0, num_outputs)
    }
}

static void splitChunk(const Chunk & chunk, IColumn::Selector & selector, size_t num_outputs, Chunks & result_chunks)
{
    const auto & columns = chunk.getColumns();
    result_chunks.resize(num_outputs);

    bool first = true;
    for (const auto & column : columns)
    {
        auto scatter = column->scatter(num_outputs, selector);

        if (first)
        {
            first = false;

            for (size_t i = 0; i < num_outputs; ++i)
            {
                size_t num_rows = scatter[i]->size();
                auto & result_chunk = result_chunks[i];
                auto res_columns = result_chunk.detachColumns();
                res_columns.clear();
                res_columns.reserve(num_outputs);
                res_columns.emplace_back(std::move(scatter[i]));

                result_chunk.setColumns(std::move(res_columns), num_rows);
            }
        }
        else
        {
            for (size_t i = 0; i < num_outputs; ++i)
                result_chunks[i].addColumn(std::move(scatter[i]));
        }
    }
}

void SplittingByHashTransform::transform(Chunk & input_chunk, Chunk & output_chunk)
{
    if (!output_chunk.getChunkInfo())
        output_chunk.setChunkInfo(std::make_shared<ChunkInfoWithChunks>());

    const auto * chunk_info = typeid_cast<const ChunkInfoWithChunks *>(output_chunk.getChunkInfo().get());
    if (!chunk_info)
        throw Exception("ResizeByHashTransform expected ChunkInfo for input chunk", ErrorCodes::LOGICAL_ERROR);

    calculateWeakHash32(input_chunk, key_columns, hash);
    fillSelector(hash, num_outputs, selector);
    splitChunk(input_chunk, selector, num_outputs, chunk_info->chunks);
}

IProcessor::Status ResizeByHashTransform::prepare()
{
    if (is_generating_phase)
        return prepareGenerate();
    else
        return prepareConsume();
}

IProcessor::Status ResizeByHashTransform::prepareConsume()
{
    auto & input = getInputPort();

    /// Check all outputs are finished or ready to get data.

    bool all_finished = true;
    for (auto & output : outputs)
    {
        if (output.isFinished())
            continue;

        all_finished = false;

        if (!output.canPush())
            return Status::PortFull;
    }

    if (all_finished)
    {
        input.close();
        return Status::Finished;
    }

    /// Try get chunk from input.

    if (input.isFinished())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    input.pull(input_chunk);

    /// Next phase after work() is generating.
    is_generating_phase = true;
    return Status::Ready;
}

IProcessor::Status ResizeByHashTransform::prepareGenerate()
{
    bool has_full_ports = false;
    bool all_outputs_processed = true;

    size_t chunk_number = 0;
    for (auto & output : outputs)
    {
        auto & chunk = output_chunks[chunk_number];
        ++chunk_number;

        if (was_output_processed[chunk_number])
            continue;

        if (!chunk.hasRows())
            continue;

        if (output.isFinished())
            continue;

        has_full_ports = true;

        if (!output.canPush())
        {
            all_outputs_processed = false;
            continue;
        }

        output.pushRef(chunk);
        was_output_processed[chunk_number] = true;
    }

    if (all_outputs_processed)
        is_generating_phase = false;

    if (has_full_ports)
        return Status::PortFull;

    /// !has_full_ports => !is_generating_phase
    /// This can happen if chunks for not finished output ports are all empty.
    return prepareConsume();
}

void ResizeByHashTransform::work()
{
    const auto * chunk_info = typeid_cast<const ChunkInfoWithChunks *>(input_chunk.getChunkInfo().get());
    if (!chunk_info)
        throw Exception("ResizeByHashTransform expected ChunkInfo for input chunk", ErrorCodes::LOGICAL_ERROR);

    output_chunks.swap(chunk_info->chunks);
    if (output_chunks.size() != outputs.size())
        throw Exception("ResizeByHashTransform expected " + std::to_string(outputs.size()) + " chunks from for input" +
                        " but got " + std::to_string(output_chunks.size()), ErrorCodes::LOGICAL_ERROR);

    was_output_processed.assign(output_chunks.size(), false);
}

}
