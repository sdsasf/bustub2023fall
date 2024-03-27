//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  rids_.clear();
  table_oid_t table_oid = plan_->GetTableOid();
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(table_oid);
  // record rid to avoid Halloween problem
  for (TableIterator i = table_info->table_->MakeIterator(); !i.IsEnd(); ++i) {
    rids_.push_back(i.GetRID());
  }
  table_iter_ = rids_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  TupleMeta meta;
  do {
    if (table_iter_ == rids_.end()) {
      return false;
    }
    meta = table_info->table_->GetTuple(*table_iter_).first;
    if (!meta.is_deleted_) {
      *tuple = table_info->table_->GetTuple(*table_iter_).second;
      *rid = *table_iter_;
    }
    ++table_iter_;
  } while (meta.is_deleted_ || (plan_->filter_predicate_ &&
                                !(plan_->filter_predicate_->Evaluate(tuple, table_info->schema_).GetAs<bool>())));
  return true;
}
}  // namespace bustub
