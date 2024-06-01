//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include <vector>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
  child_executor_->Init();
  txn_mgr_ = exec_ctx_->GetTransactionManager();
  txn_ = exec_ctx_->GetTransaction();
  is_called_ = false;
}

// rid should be unused
auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  int sz = 0;
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->GetTableOid());
  // only have primary key index in P4
  std::vector<IndexInfo *> indexes = catalog->GetTableIndexes(table_info->name_);
  IndexInfo *primary_key_idx_info = indexes.empty() ? nullptr : indexes[0];
  while (child_executor_->Next(&child_tuple, rid)) {
    // if don't have primary key index
    // every insert allocate a new rid, don't need to LockVersionLink()
    if (primary_key_idx_info == nullptr) {
      auto rid_optional = table_info->table_->InsertTuple(TupleMeta{txn_->GetTransactionTempTs(), false}, child_tuple,
                                                          exec_ctx_->GetLockManager(), txn_, plan_->GetTableOid());
      txn_mgr_->UpdateUndoLink(*rid_optional, UndoLink());
      txn_->AppendWriteSet(table_info->oid_, *rid_optional);
    } else {
      InsertTuple(primary_key_idx_info, table_info, txn_mgr_, txn_, exec_ctx_->GetLockManager(), child_tuple,
                  &child_executor_->GetOutputSchema());
    }
    ++sz;
  }
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, sz);
  *tuple = Tuple(values, &GetOutputSchema());
  // even if insert no value, must return true first
  if (sz == 0 && !is_called_) {
    is_called_ = true;
    return true;
  }
  is_called_ = true;
  return sz != 0;
}

}  // namespace bustub
