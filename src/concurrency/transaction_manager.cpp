//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (txn->GetTransactionState() == TransactionState::TAINTED) {
    return false;
  }
  // don't check read-only txn
  if (txn->GetWriteSets().empty()) {
    // std::cerr << " Txn " << txn->GetTransactionIdHumanReadable() << " write set is empty " << std::endl;
    return true;
  }
  std::unordered_map<table_oid_t, std::unordered_set<RID>> rids;
  const timestamp_t read_ts = txn->GetReadTs();
  // go through txn_map_ to find conflict txn and collect write set
  std::shared_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (auto i = txn_map_.begin(); i != txn_map_.end(); ++i) {
    std::shared_ptr<Transaction> temp_txn = i->second;
    if ((temp_txn->GetTransactionState() == TransactionState::COMMITTED) && (temp_txn->GetCommitTs() > read_ts)) {
      // std::cerr << "txn " << temp_txn->GetTransactionIdHumanReadable() << " is checked " << std::endl;
      const auto &temp_write_set = temp_txn->GetWriteSets();
      for (auto j = temp_write_set.cbegin(); j != temp_write_set.cend(); ++j) {
        for (auto k = j->second.cbegin(); k != j->second.cend(); ++k) {
          // std::cerr << "tuple rid " << *k << " is checked" << std::endl;
          rids[j->first].insert(*k);
        }
      }
    }
  }
  lck.unlock();

  for (auto i = rids.begin(); i != rids.end(); ++i) {
    TableInfo *table_info = catalog_->GetTable(i->first);
    for (auto rid_iter = i->second.begin(); rid_iter != i->second.end(); ++rid_iter) {
      auto tuple_pair = table_info->table_->GetTuple(*rid_iter);
      // std::cerr << " Check tuple rid is " << *rid_iter << std::endl;
      UndoLink undo_link = *GetUndoLink(*rid_iter);
      if (tuple_pair.first.ts_ < TXN_START_ID) {
        // need check tuple in table heap
        if (!tuple_pair.first.is_deleted_) {
          // if is not insert and delete by the same txn
          if (CheckOverlap(txn->scan_predicates_[table_info->oid_], &tuple_pair.second, table_info->schema_)) {
            // Abort(txn);
            return false;
          }
        }
      }

      // check each version one by one
      std::optional<Tuple> res_tuple = tuple_pair.second;
      while (undo_link.IsValid()) {
        auto undo_log = GetUndoLog(undo_link);
        if (undo_log.ts_ < txn->GetReadTs()) {
          break;
        }
        if (undo_log.is_deleted_) {
          undo_link = undo_log.prev_version_;
          continue;
        }
        res_tuple = ReplayUndoLog(&table_info->schema_, *res_tuple, undo_log);
        if (CheckOverlap(txn->scan_predicates_[table_info->oid_], &tuple_pair.second, table_info->schema_)) {
          // Abort(txn);
          return false;
        }

        undo_link = undo_log.prev_version_;
      }
    }
  }
  return true;
}

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  timestamp_t temp_commit_ts = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  // set the timestamp of the base tuples to the commit timestamp
  const std::unordered_map<table_oid_t, std::unordered_set<RID>> &txn_write_set = txn->GetWriteSets();
  for (const auto &i : txn_write_set) {
    TableInfo *temp_table_info = catalog_->GetTable(i.first);
    for (auto &j : i.second) {
      TupleMeta old_meta = temp_table_info->table_->GetTupleMeta(j);
      temp_table_info->table_->UpdateTupleMeta({temp_commit_ts, old_meta.is_deleted_}, j);
      UnsetInProgress(j, this);
      // std::cerr << "txn " << txn->GetTransactionIdHumanReadable() << " set in_progress_ = 0" << std::endl;
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  txn->state_ = TransactionState::COMMITTED;
  txn->commit_ts_ = temp_commit_ts;

  // update last_commit_ts in txn_mgr_
  ++last_commit_ts_;
  // update watermark
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!
  const std::unordered_map<table_oid_t, std::unordered_set<RID>> &txn_write_set = txn->GetWriteSets();
  // revert write set
  for (const auto &i : txn_write_set) {
    TableInfo *temp_table_info = catalog_->GetTable(i.first);
    for (auto &j : i.second) {
      auto undo_link_optional = GetUndoLink(j);
      if (!undo_link_optional.has_value()) std::cerr << "undo_link is empty!" << std::endl;
      if (undo_link_optional->IsValid()) {
        auto undo_log_optional = GetUndoLogOptional(*undo_link_optional);
        auto tuple_pair = temp_table_info->table_->GetTuple(j);
        if (!undo_log_optional.has_value()) std::cerr << "undo_log is empty!" << std::endl;
        auto undo_log = *undo_log_optional;
        auto origin_tuple = ReplayUndoLog(&temp_table_info->schema_, tuple_pair.second, undo_log);
        // auto origin_tuple = ReconstructTuple(&temp_table_info->schema_, tuple_pair.second, tuple_pair.first,
        // undo_logs);
        // std::unique_lock<std::mutex> commit_lck(commit_mutex_);
        if (origin_tuple.has_value()) {
          temp_table_info->table_->UpdateTupleInPlace(TupleMeta{undo_log.ts_, undo_log.is_deleted_}, *origin_tuple, j);
        } else {
          temp_table_info->table_->UpdateTupleMeta(TupleMeta{undo_log.ts_, true}, j);
        }
      } else {
        // std::unique_lock<std::mutex> commit_lck(commit_mutex_);
        temp_table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, j);
      }
      UnsetInProgress(j, this);
      // std::cerr << "txn " << txn->GetTransactionIdHumanReadable() << " set in_progress_ = 0" << std::endl;
    }
  }

  // std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // UNIMPLEMENTED("not implemented");
  std::unordered_set<txn_id_t> txn_set;
  std::vector<std::string> table_names = catalog_->GetTableNames();
  timestamp_t water_mark = GetWatermark();
  for (const auto &name : table_names) {
    TableInfo *table_info = catalog_->GetTable(name);
    TableIterator table_iter = table_info->table_->MakeIterator();
    while (!table_iter.IsEnd()) {
      const auto &[meta, tuple] = table_iter.GetTuple();
      if (meta.ts_ > water_mark) {
        // find version link head
        std::optional<UndoLink> undo_link_optional = GetUndoLink(tuple.GetRid());
        // if tuple has a version link
        if (undo_link_optional.has_value()) {
          // while have undo log
          bool is_not_first = false;
          while (undo_link_optional.value().IsValid()) {
            std::optional<UndoLog> undo_log_optinal = GetUndoLogOptional(undo_link_optional.value());
            if (undo_log_optinal.has_value()) {
              if (undo_log_optinal.value().ts_ <= water_mark) {
                if (is_not_first) {
                  // fmt::println(stderr, "is not first is {}, txn id {}", is_not_first,
                  // undo_link_optional.value().prev_txn_ ^ TXN_START_ID);
                  break;
                }
                // fmt::println(stderr, "set is not first {}", undo_link_optional.value().prev_txn_ ^ TXN_START_ID);
                is_not_first = true;
              }
              txn_id_t txn_id = undo_link_optional.value().prev_txn_;
              if (txn_set.count(txn_id) == 0) {
                // fmt::println(stderr, "insert txn{}", txn_id ^ TXN_START_ID);
                txn_set.insert(txn_id);
              }
              undo_link_optional = undo_log_optinal.value().prev_version_;
            } else {
              break;
            }
          }
        }
      }
      ++table_iter;
    }
  }

  std::unique_lock<std::shared_mutex> lk(txn_map_mutex_);
  for (auto i = txn_map_.begin(); i != txn_map_.end();) {
    if (txn_set.count(i->first) == 0 && ((i->second->GetTransactionState() == TransactionState::COMMITTED) ||
                                         (i->second->GetTransactionState() == TransactionState::ABORTED))) {
      // fmt::println(stderr, "delete txn{}", i->first ^ TXN_START_ID);
      txn_map_.erase(i++);
    } else {
      ++i;
    }
  }
}

}  // namespace bustub
