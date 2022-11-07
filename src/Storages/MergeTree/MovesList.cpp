#include <Storages/MergeTree/MovesList.h>
#include <Common/CurrentMetrics.h>
#include <base/getThreadId.h>

namespace DB
{

MovesListElement::MovesListElement(
        const StorageID & table_id_,
        const std::string & partition_id_,
        const std::string & target_disk_path_,
        UInt64 part_size_)
    : table_id(table_id_)
    , partition_id(partition_id_)
    , target_disk_path(target_disk_path_)
    , part_size(part_size_)
    , thread_id(getThreadId())
{
}

MoveInfo MovesListElement::getInfo() const
{
    MoveInfo res;
    res.database = table_id.database_name;
    res.table = table_id.table_name;
    res.partition_id = partition_id;
    res.target_disk_path = target_disk_path;
    res.part_size = part_size;
    res.elapsed = watch.elapsedSeconds();
    res.thread_id = thread_id;
    return res;
}

}
