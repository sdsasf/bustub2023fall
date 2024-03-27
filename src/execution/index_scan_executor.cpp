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
#include <vector>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // throw NotImplementedException("IndexScanExecutor is not implemented");
  rids_.clear();
  IndexInfo *index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->index_oid_);
  // as document said, it's safe
  auto *hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  if (plan_->filter_predicate_) {
    // assume only one Equal expr use index_scan_executor, so I use children_[1] as the Value
    const auto *expr = dynamic_cast<ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
    Tuple target_tuple(std::vector<Value>{expr->val_}, &(index_info->key_schema_));
    hash_index->ScanKey(target_tuple, &rids_, nullptr);
  }
  rids_iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
  TupleMeta meta;
  do {
    if (rids_iter_ == rids_.end()) {
      return false;
    }
    *rid = *rids_iter_;
    meta = table_info->table_->GetTupleMeta(*rid);
    *tuple = table_info->table_->GetTuple(*rid).second;
    ++rids_iter_;
  } while (meta.is_deleted_);
  return true;
}

}  // namespace bustub
