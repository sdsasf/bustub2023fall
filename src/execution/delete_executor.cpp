//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include <queue>
#include <vector>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented");
  child_executor_->Init();
  is_called_ = false;

  txn_mgr_ = exec_ctx_->GetTransactionManager();
  txn_ = exec_ctx_->GetTransaction();
  Tuple old_tuple{};
  RID rid;
  while (child_executor_->Next(&old_tuple, &rid)) {
    buffer_.push({rid, old_tuple});
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple delete_tuple{};
  RID temp_rid;
  int delete_num = 0;
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->GetTableOid());
  std::vector<IndexInfo *> indexes = catalog->GetTableIndexes(table_info->name_);
  // origin tuple schema
  Schema schema = child_executor_->GetOutputSchema();
  while (!buffer_.empty()) {
    auto tuple_pair = buffer_.front();
    delete_tuple = std::move(tuple_pair.second);
    temp_rid = tuple_pair.first;
    TupleMeta old_tuple_meta = table_info->table_->GetTupleMeta(temp_rid);
    buffer_.pop();

    DeleteTuple(table_info, &schema, txn_mgr_, txn_, old_tuple_meta, delete_tuple, temp_rid);

    ++delete_num;
  }
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, delete_num);
  *tuple = Tuple(values, &GetOutputSchema());
  if (delete_num == 0 && !is_called_) {
    is_called_ = true;
    return true;
  }
  is_called_ = true;
  return delete_num != 0;
}

}  // namespace bustub
