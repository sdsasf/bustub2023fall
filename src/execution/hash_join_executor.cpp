//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <algorithm>
#include <vector>

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // throw NotImplementedException("HashJoinExecutor is not implemented");
  left_executor_->Init();
  right_executor_->Init();

  // init entry
  hash_table_.clear();
  queue_ = std::queue<Tuple>();

  Tuple right_tuple;
  RID right_rid;
  // insert all tuples in right table into hash table
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    hash_table_[GetRightJoinKey(right_tuple)].push(right_tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!queue_.empty()) {
      *tuple = InnerJoinTuple(&left_tuple_, &(queue_.front()));
      *rid = tuple->GetRid();
      queue_.pop();
      return true;
    }
    // if queue is empty, should use next tuple in left table
    while (true) {
      RID left_rid;
      if (!left_executor_->Next(&left_tuple_, &left_rid)) {
        return false;
      }
      // if not match
      if (hash_table_.count(GetLeftJoinKey(left_tuple_)) == 0) {
        if (plan_->GetJoinType() == JoinType::LEFT) {
          // left join must return left tuple
          *tuple = LeftJoinTuple(&left_tuple_);
          *rid = tuple->GetRid();
          return true;
        }
        continue;
      }
      // if match
      // std::queue<Tuple> queue_ = hash_table_[GetLeftJoinKey(left_tuple_)];
      // temp varable shadow the entry in the class
      queue_ = hash_table_[GetLeftJoinKey(left_tuple_)];
      break;
    }
  }
}

auto HashJoinExecutor::GetLeftJoinKey(const Tuple &tuple) -> HashJoinKey {
  std::vector<Value> values;
  const std::vector<AbstractExpressionRef> &left_key_expr = plan_->LeftJoinKeyExpressions();
  for (const auto &expr : left_key_expr) {
    values.push_back(expr->Evaluate(&tuple, left_executor_->GetOutputSchema()));
  }
  return {values};
}

auto HashJoinExecutor::GetRightJoinKey(const Tuple &tuple) -> HashJoinKey {
  std::vector<Value> values;
  const std::vector<AbstractExpressionRef> &right_key_expr = plan_->RightJoinKeyExpressions();
  for (const auto &expr : right_key_expr) {
    values.push_back(expr->Evaluate(&tuple, right_executor_->GetOutputSchema()));
  }
  return {values};
}

auto HashJoinExecutor::InnerJoinTuple(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(left_tuple->GetValue(&(left_executor_->GetOutputSchema()), i));
  }
  for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(right_tuple->GetValue(&(right_executor_->GetOutputSchema()), i));
  }
  return {values, &GetOutputSchema()};
}

auto HashJoinExecutor::LeftJoinTuple(const Tuple *left_tuple) -> Tuple {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(left_tuple->GetValue(&(left_executor_->GetOutputSchema()), i));
  }
  for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
  }
  return {values, &GetOutputSchema()};
}

}  // namespace bustub
