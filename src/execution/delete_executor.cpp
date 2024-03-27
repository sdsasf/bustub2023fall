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

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented");
  child_executor_->Init();
  is_called_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple delete_tuple{};
  int delete_num = 0;
  ExecutorContext *ctx = GetExecutorContext();
  Catalog *catalog = ctx->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->GetTableOid());
  std::vector<IndexInfo *> indexes = catalog->GetTableIndexes(table_info->name_);
  while (child_executor_->Next(&delete_tuple, rid)) {
    // delete tuple
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);
    // update associated index
    for (auto idx_info : indexes) {
      idx_info->index_->DeleteEntry(
          delete_tuple.KeyFromTuple(table_info->schema_, idx_info->key_schema_, idx_info->index_->GetKeyAttrs()), *rid,
          nullptr);
    }
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
