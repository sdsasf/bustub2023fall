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
#include <memory>
#include <optional>
#include <vector>
#include "catalog/schema.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  table_oid_t table_oid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid);
  txn_ = exec_ctx_->GetTransaction();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
  // table_iter could avoid Halloween problem by recording the stop point RID
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());

  // add predicate to txn every time when executor was called
  exec_ctx_->GetTransaction()->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);
  // std::cerr << "predicate append in txn " << exec_ctx_->GetTransaction()->GetTransactionIdHumanReadable() <<
  // std::endl;
}

// seq_scan must return valid rid, although tuple is reconstructed
// because mvcc executors will use rid to update tuple in table heap
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  bool is_find = false;
  do {
    is_find = false;
    if (table_iter_->IsEnd()) {
      return false;
    }
    const std::pair<TupleMeta, Tuple> &tuple_pair = table_iter_->GetTuple();
    if (tuple_pair.first.ts_ == txn_->GetTransactionTempTs()) {
      if (tuple_pair.first.is_deleted_) {
        // is delete by this txn, can't read by this txn
        ++(*table_iter_);
        continue;
      }
      // is modifying by this txn
      *tuple = tuple_pair.second;
      *rid = tuple->GetRid();
      is_find = true;
    } else {
      // else need timestamp
      timestamp_t txn_ts = txn_->GetReadTs();
      timestamp_t tuple_ts = tuple_pair.first.ts_;
      std::vector<UndoLog> undo_logs;

      // need find undoLog through transaction manager, else don't need
      if (txn_ts < tuple_ts) {
        // find undoLink head
        std::optional<UndoLink> undo_link_optional = txn_mgr_->GetUndoLink(tuple_pair.second.GetRid());
        if (undo_link_optional.has_value()) {
          // while have next undolog
          while (undo_link_optional.has_value() && undo_link_optional.value().IsValid()) {
            std::optional<UndoLog> undo_log_optional = txn_mgr_->GetUndoLogOptional(undo_link_optional.value());
            if (undo_log_optional.has_value()) {
              if (txn_ts >= undo_log_optional.value().ts_) {
                undo_logs.push_back(std::move(undo_log_optional.value()));
                std::optional<Tuple> res_tuple_optional =
                    ReconstructTuple(&GetOutputSchema(), tuple_pair.second, tuple_pair.first, undo_logs);
                if (res_tuple_optional.has_value()) {
                  *tuple = res_tuple_optional.value();
                  // although tuple is reconstructed, return valid rid in table heap
                  *rid = tuple_pair.second.GetRid();
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
        // return tuple in table heap directly
        if (!tuple_pair.first.is_deleted_) {
          is_find = true;
          *tuple = tuple_pair.second;
          *rid = tuple->GetRid();
        }
      }
    }
    ++(*table_iter_);
  } while (!is_find || (plan_->filter_predicate_ &&
                        !(plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>())));
  return true;
}
}  // namespace bustub
