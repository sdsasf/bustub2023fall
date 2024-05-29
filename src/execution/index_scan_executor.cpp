//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <optional>
#include <vector>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // throw NotImplementedException("IndexScanExecutor is not implemented");
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();

  rids_.clear();
  // only have primary index
  IndexInfo *primary_key_idx_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  // as document said, it's safe
  // auto *hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  if (plan_->filter_predicate_) {
    // assume only one Equal expr use index_scan_executor, so I use children_[1] as the Value
    const auto *expr = dynamic_cast<ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
    Tuple target_tuple(std::vector<Value>{expr->val_}, &(primary_key_idx_info->key_schema_));
    primary_key_idx_info->index_->ScanKey(target_tuple, &rids_, txn_);
  }

  rids_iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  bool is_find = false;
  do {
    is_find = false;
    if (rids_iter_ == rids_.end()) {
      return false;
    }
    const auto &[meta, temp_tuple] = table_info->table_->GetTuple(*rids_iter_);
    // if self-modification
    if (meta.ts_ == txn_->GetTransactionTempTs()) {
      if (meta.is_deleted_) {
        ++rids_iter_;
        continue;
      }
      is_find = true;
      *tuple = temp_tuple;
      *rid = *rids_iter_;
    } else {
      timestamp_t txn_ts = txn_->GetReadTs();
      timestamp_t tuple_ts = meta.ts_;
      std::vector<UndoLog> undo_logs;

      if (txn_ts < tuple_ts) {
        std::optional<UndoLink> undo_link_optional = txn_mgr_->GetUndoLink(*rids_iter_);
        if (undo_link_optional.has_value()) {
          while (undo_link_optional.value().IsValid()) {
            std::optional<UndoLog> undo_log_optional = txn_mgr_->GetUndoLogOptional(undo_link_optional.value());
            if (undo_log_optional.has_value()) {
              if (txn_ts >= undo_log_optional.value().ts_) {
                undo_logs.push_back(std::move(undo_log_optional.value()));
                std::optional<Tuple> res_tuple_optional =
                    ReconstructTuple(&GetOutputSchema(), temp_tuple, meta, undo_logs);
                if (res_tuple_optional.has_value()) {
                  *tuple = res_tuple_optional.value();
                  *rid = *rids_iter_;
                  is_find = true;
                }
                break;
              }
              undo_link_optional = undo_log_optional.value().prev_version_;
              undo_logs.push_back(std::move(undo_log_optional.value()));
            }
          }
        }
      } else {
        if (!meta.is_deleted_) {
          is_find = true;
          *tuple = temp_tuple;
          *rid = *rids_iter_;
        }
      }
    }
    ++rids_iter_;
  } while (!is_find);
  return true;
}

}  // namespace bustub
