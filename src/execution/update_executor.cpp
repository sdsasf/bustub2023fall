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
#include <optional>
#include <vector>

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
  buffer_.clear();

  txn_mgr_ = exec_ctx_->GetTransactionManager();
  txn_ = exec_ctx_->GetTransaction();
  Tuple old_tuple{};
  RID rid;
  TupleMeta temp_meta;
  while (child_executor_->Next(&old_tuple, &rid)) {
    // use buffered old tuple because Visibility is guaranteed by the child executor
    if (rid.GetPageId() != INVALID_PAGE_ID) {
      buffer_.emplace_back(rid, old_tuple);
    }
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  uint32_t i = 0;

  Tuple old_tuple{};
  TupleMeta old_tuple_meta;
  RID temp_rid;
  int update_num = 0;
  std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  IndexInfo *primary_key_idx_info = indexes.front();
  auto key_idx = primary_key_idx_info->index_->GetKeyAttrs().front();
  // std::cerr << "key index is  " << key_idx << std::endl;
  const Schema *schema = &child_executor_->GetOutputSchema();
  // if is primary key update
  if (const auto *cons_expr = dynamic_cast<const ColumnValueExpression *>(plan_->target_expressions_[key_idx].get());
      cons_expr == nullptr) {
    // delete all buffered tuple first
    for (const auto &tuple_pair : buffer_) {
      temp_rid = tuple_pair.first;
      old_tuple = tuple_pair.second;
      old_tuple_meta = table_info_->table_->GetTupleMeta(temp_rid);

      DeleteTuple(table_info_, schema, txn_mgr_, txn_, old_tuple_meta, old_tuple, temp_rid);
    }
    std::vector<Value> values;
    while (!buffer_.empty()) {
      values.clear();
      auto tuple_pair = buffer_.front();
      temp_rid = tuple_pair.first;
      old_tuple = std::move(tuple_pair.second);
      buffer_.pop_front();

      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&old_tuple, *schema));
      }
      Tuple new_tuple = Tuple(values, schema);
      InsertTuple(primary_key_idx_info, table_info_, txn_mgr_, txn_, exec_ctx_->GetLockManager(), new_tuple, schema);

      txn_->AppendWriteSet(table_info_->oid_, temp_rid);
      ++update_num;
    }
  } else {
    const TupleMeta new_tuple_meta = TupleMeta{txn_->GetTransactionTempTs(), false};
    std::vector<Value> values;
    values.reserve(schema->GetColumnCount());
    while (!buffer_.empty()) {
      values.clear();
      auto tuple_pair = buffer_.front();
      temp_rid = tuple_pair.first;
      old_tuple = std::move(tuple_pair.second);
      buffer_.pop_front();

      // update column values according to expressions
      for (const auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&old_tuple, *schema));
      }
      Tuple new_tuple = Tuple(values, schema);
      old_tuple_meta = table_info_->table_->GetTupleMeta(temp_rid);

      // if self-modification
      // fast path, don't need lock
      if (old_tuple_meta.ts_ == txn_->GetTransactionTempTs()) {
        std::optional<UndoLink> undo_link_optional = txn_mgr_->GetUndoLink(temp_rid);
        if (undo_link_optional->IsValid()) {
          UndoLog temp_undo_log = GenerateDiffLog(old_tuple, old_tuple_meta, new_tuple, new_tuple_meta, schema);
          UndoLog old_undo_log = txn_mgr_->GetUndoLogOptional(undo_link_optional.value()).value();
          temp_undo_log.prev_version_ = old_undo_log.prev_version_;
          UndoLog merged_undo_log = MergeUndoLog(temp_undo_log, old_undo_log, schema);
          txn_->ModifyUndoLog(undo_link_optional.value().prev_log_idx_, merged_undo_log);
        }
      } else {
        LockAndCheck(temp_rid, txn_mgr_, txn_, table_info_);

        std::optional<UndoLink> undo_link_optional = txn_mgr_->GetUndoLink(temp_rid);
        UndoLog temp_undo_log = GenerateDiffLog(old_tuple, old_tuple_meta, new_tuple, new_tuple_meta, schema);

        temp_undo_log.prev_version_ = *undo_link_optional;

        UndoLink new_undo_link = txn_->AppendUndoLog(temp_undo_log);
        // txn_mgr_->UpdateVersionLink(temp_rid, VersionUndoLink{std::move(new_undo_link), true}, nullptr);
        txn_mgr_->UpdateUndoLink(temp_rid, new_undo_link, nullptr);
      }

      // if is self_modification and have no version link(inserted by this txn)
      // only update table heap
      table_info_->table_->UpdateTupleInPlace(new_tuple_meta, new_tuple, temp_rid, nullptr);
      txn_->AppendWriteSet(table_info_->oid_, temp_rid);

      ++update_num;
    }
  }
  std::vector<Value> return_tuple;
  return_tuple.reserve(GetOutputSchema().GetColumnCount());
  return_tuple.emplace_back(TypeId::INTEGER, update_num);
  *tuple = Tuple(return_tuple, &GetOutputSchema());
  if (update_num == 0 && !is_called_) {
    is_called_ = true;
    return true;
  }
  is_called_ = true;
  return update_num != 0;
}

}  // namespace bustub
