#pragma once
#include <Processors/IProcessor.h>
#include <Processors/ISimpleTransform.h>
#include <Core/ColumnNumbers.h>
#include <Common/WeakHash.h>

namespace DB
{

/// Split single chunks to num_outputs chunks.
/// Split is performed by formula (WeakHash(key_columns) * num_outputs / MAX_INT).
/// All this chunks are returned via single output port. Use ResizeByHashTransform to divide this chunks between output ports.
class SplittingByHashTransform : public ISimpleTransform
{
public:
    SplittingByHashTransform(const Block & header, size_t num_outputs_, ColumnNumbers key_columns_);
    String getName() const override { return "SplittingByHash"; }
    void transform(Chunk & input_chunk, Chunk & output_chunk) override;

private:
    size_t num_outputs;
    ColumnNumbers key_columns;

    IColumn::Selector selector;
    IColumn::Filter filter;
    WeakHash32 hash;
};

/// Resize input from SplittingByHashTransform to different output ports.
class ResizeByHashTransform : public IProcessor
{
public:
    ResizeByHashTransform(const Block & header, size_t num_outputs);

    String getName() const override { return "ResizeByHash"; }
    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return inputs.front(); }

private:
    Chunk input_chunk;
    Chunks output_chunks;
    std::vector<char> was_output_processed;

    bool is_generating_phase = false;

    Status prepareGenerate();
    Status prepareConsume();
};

}
