#include <Storages/StorageInMemoryMetadata.h>
#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

#include <IO/WriteHelpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
    extern const int DUPLICATE_COLUMN;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int TYPE_MISMATCH;
    extern const int TABLE_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
    extern const int DEADLOCK_AVOIDED;
}


StorageInMemoryMetadata::StorageInMemoryMetadata(
    const ColumnsDescription & columns_,
    const IndicesDescription & indices_,
    const ConstraintsDescription & constraints_)
    : columns(columns_)
    , indices(indices_)
    , constraints(constraints_)
{
}

StorageInMemoryMetadata::StorageInMemoryMetadata(const StorageInMemoryMetadata & other)
    : columns(other.columns)
    , indices(other.indices)
    , constraints(other.constraints)
{
    if (other.partition_by_ast)
        partition_by_ast = other.partition_by_ast->clone();
    if (other.order_by_ast)
        order_by_ast = other.order_by_ast->clone();
    if (other.primary_key_ast)
        primary_key_ast = other.primary_key_ast->clone();
    if (other.ttl_for_table_ast)
        ttl_for_table_ast = other.ttl_for_table_ast->clone();
    if (other.sample_by_ast)
        sample_by_ast = other.sample_by_ast->clone();
    if (other.settings_ast)
        settings_ast = other.settings_ast->clone();
    if (other.select)
        select = other.select->clone();
}

StorageInMemoryMetadata & StorageInMemoryMetadata::operator=(const StorageInMemoryMetadata & other)
{
    if (this == &other)
        return *this;

    columns = other.columns;
    indices = other.indices;
    constraints = other.constraints;

    if (other.partition_by_ast)
        partition_by_ast = other.partition_by_ast->clone();
    else
        partition_by_ast.reset();

    if (other.order_by_ast)
        order_by_ast = other.order_by_ast->clone();
    else
        order_by_ast.reset();

    if (other.primary_key_ast)
        primary_key_ast = other.primary_key_ast->clone();
    else
        primary_key_ast.reset();

    if (other.ttl_for_table_ast)
        ttl_for_table_ast = other.ttl_for_table_ast->clone();
    else
        ttl_for_table_ast.reset();

    if (other.sample_by_ast)
        sample_by_ast = other.sample_by_ast->clone();
    else
        sample_by_ast.reset();

    if (other.settings_ast)
        settings_ast = other.settings_ast->clone();
    else
        settings_ast.reset();

    if (other.select)
        select = other.select->clone();
    else
        select.reset();

    return *this;
}

const ColumnsDescription & StorageInMemoryMetadata::getColumns() const
{
    return columns;
}

Block StorageInMemoryMetadata::getSampleBlock() const
{
    Block res;

    for (const auto & column : getColumns().getAllPhysical())
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}
Block StorageInMemoryMetadata::getSampleBlockWithVirtuals(const NamesAndTypesList & virtuals) const
{
    auto res = getSampleBlock();

    for (const auto & column : virtuals)
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

Block StorageInMemoryMetadata::getSampleBlockNonMaterialized() const
{
    Block res;

    for (const auto & column : getColumns().getOrdinary())
        res.insert({column.type->createColumn(), column.type, column.name});

    return res;
}

Block StorageInMemoryMetadata::getSampleBlockForColumns(const Names & column_names, const NamesAndTypesList & virtual_columns) const
{
    Block res;

    NamesAndTypesList all_columns = getColumns().getAll();
    std::unordered_map<String, DataTypePtr> columns_map;

    for (const auto & column : virtual_columns)
        columns_map.emplace(column.name, column.type);

    for (const auto & elem : all_columns)
        columns_map.emplace(elem.name, elem.type);

    for (const auto & name : column_names)
    {
        auto it = columns_map.find(name);
        if (it != columns_map.end())
        {
            res.insert({it->second->createColumn(), it->second, it->first});
        }
        else
        {
            throw Exception(
                "Column " + backQuote(name) + " not found in table ",
                ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
    }

    return res;
}

namespace
{
#if !defined(ARCADIA_BUILD)
    using NamesAndTypesMap = google::dense_hash_map<StringRef, const IDataType *, StringRefHash>;
    using UniqueStrings = google::dense_hash_set<StringRef, StringRefHash>;
#else
    using NamesAndTypesMap = google::sparsehash::dense_hash_map<StringRef, const IDataType *, StringRefHash>;
    using UniqueStrings = google::sparsehash::dense_hash_set<StringRef, StringRefHash>;
#endif

    String listOfColumns(const NamesAndTypesList & available_columns)
    {
        std::stringstream ss;
        for (auto it = available_columns.begin(); it != available_columns.end(); ++it)
        {
            if (it != available_columns.begin())
                ss << ", ";
            ss << it->name;
        }
        return ss.str();
    }

    NamesAndTypesMap getColumnsMap(const NamesAndTypesList & columns)
    {
        NamesAndTypesMap res;
        res.set_empty_key(StringRef());

        for (const auto & column : columns)
            res.insert({column.name, column.type.get()});

        return res;
    }

    UniqueStrings initUniqueStrings()
    {
        UniqueStrings strings;
        strings.set_empty_key(StringRef());
        return strings;
    }
}

void StorageInMemoryMetadata::check(const Names & column_names, const NamesAndTypesList & virtuals) const
{
    NamesAndTypesList available_columns = getColumns().getAllPhysical();
    available_columns.insert(available_columns.end(), virtuals.begin(), virtuals.end());

    const String list_of_columns = listOfColumns(available_columns);

    if (column_names.empty())
        throw Exception("Empty list of columns queried. There are columns: " + list_of_columns, ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

    const auto columns_map = getColumnsMap(available_columns);

    auto unique_names = initUniqueStrings();
    for (const auto & name : column_names)
    {
        if (columns_map.end() == columns_map.find(name))
            //TODO(alesap)
            throw Exception(
                "There is no column with name " + backQuote(name) + ". There are columns: " + list_of_columns,
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (unique_names.end() != unique_names.find(name))
            throw Exception("Column " + name + " queried more than once", ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(name);
    }
}

void StorageInMemoryMetadata::check(const NamesAndTypesList & provided_columns) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto columns_map = getColumnsMap(available_columns);

    auto unique_names = initUniqueStrings();
    for (const NameAndTypePair & column : provided_columns)
    {
        auto it = columns_map.find(column.name);
        if (columns_map.end() == it)
            throw Exception(
                "There is no column with name " + column.name + ". There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!column.type->equals(*it->second))
            throw Exception(
                "Type mismatch for column " + column.name + ". Column has type " + it->second->getName() + ", got type "
                    + column.type->getName(),
                ErrorCodes::TYPE_MISMATCH);

        if (unique_names.end() != unique_names.find(column.name))
            throw Exception("Column " + column.name + " queried more than once", ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(column.name);
    }
}

void StorageInMemoryMetadata::check(const NamesAndTypesList & provided_columns, const Names & column_names) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto available_columns_map = getColumnsMap(available_columns);
    const auto & provided_columns_map = getColumnsMap(provided_columns);

    if (column_names.empty())
        throw Exception(
            "Empty list of columns queried. There are columns: " + listOfColumns(available_columns),
            ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);

    auto unique_names = initUniqueStrings();
    for (const String & name : column_names)
    {
        auto it = provided_columns_map.find(name);
        if (provided_columns_map.end() == it)
            continue;

        auto jt = available_columns_map.find(name);
        if (available_columns_map.end() == jt)
            throw Exception(
                "There is no column with name " + name + ". There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!it->second->equals(*jt->second))
            throw Exception(
                "Type mismatch for column " + name + ". Column has type " + jt->second->getName() + ", got type " + it->second->getName(),
                ErrorCodes::TYPE_MISMATCH);

        if (unique_names.end() != unique_names.find(name))
            throw Exception("Column " + name + " queried more than once", ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE);
        unique_names.insert(name);
    }
}

void StorageInMemoryMetadata::check(const Block & block, bool need_all) const
{
    const NamesAndTypesList & available_columns = getColumns().getAllPhysical();
    const auto columns_map = getColumnsMap(available_columns);

    NameSet names_in_block;

    block.checkNumberOfRows();

    for (const auto & column : block)
    {
        if (names_in_block.count(column.name))
            throw Exception("Duplicate column " + column.name + " in block", ErrorCodes::DUPLICATE_COLUMN);

        names_in_block.insert(column.name);

        auto it = columns_map.find(column.name);
        if (columns_map.end() == it)
            throw Exception(
                "There is no column with name " + column.name + ". There are columns: " + listOfColumns(available_columns),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!column.type->equals(*it->second))
            throw Exception(
                "Type mismatch for column " + column.name + ". Column has type " + it->second->getName() + ", got type "
                    + column.type->getName(),
                ErrorCodes::TYPE_MISMATCH);
    }

    if (need_all && names_in_block.size() < columns_map.size())
    {
        for (const auto & available_column : available_columns)
        {
            if (!names_in_block.count(available_column.name))
                throw Exception("Expected column " + available_column.name, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
    }
}

void StorageInMemoryMetadata::setColumns(ColumnsDescription columns_)
{
    if (columns_.getOrdinary().empty())
        throw Exception("Empty list of columns passed", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
    columns = std::move(columns_);
}


}
