//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  child_executor_->Init();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  is_called_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple old_tuple{};
  int update_num = 0;
  ExecutorContext *ctx = GetExecutorContext();
  Catalog *catalog = ctx->GetCatalog();
  std::vector<IndexInfo *> indexes = catalog->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(&old_tuple, rid)) {
    std::vector<Value> values;
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    // delete tuple first, modify tupleMeta is_delete = true
    // lazy delete
    table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);
    // update column values according to expressions
    for (auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple = Tuple(values, &(child_executor_->GetOutputSchema()));
    // delete tuple and insert a new tuple, the rid is different
    // so must use new rid to insert
    auto new_rid_optional = table_info_->table_->InsertTuple(TupleMeta{0, false}, new_tuple, ctx->GetLockManager(),
                                                             nullptr, table_info_->oid_);
    // use table_heap to insert/delete tuple
    RID new_rid = new_rid_optional.value();
    // update associated index
    for (auto idx_info : indexes) {
      // delete index first
      idx_info->index_->DeleteEntry(
          old_tuple.KeyFromTuple(table_info_->schema_, idx_info->key_schema_, idx_info->index_->GetKeyAttrs()), *rid,
          nullptr);
      // insert new tuple's index
      idx_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info_->schema_, idx_info->key_schema_, idx_info->index_->GetKeyAttrs()), new_rid,
          nullptr);
    }
    ++update_num;
  }
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, update_num);
  *tuple = Tuple(values, &GetOutputSchema());
  if (update_num == 0 && !is_called_) {
    is_called_ = true;
    return true;
  }
  is_called_ = true;
  return update_num != 0;
}

}  // namespace bustub
