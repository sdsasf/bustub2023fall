//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
  left_executor_->Init();
  right_executor_->Init();

  /*Tuple left_tuple;
  Tuple right_tuple;
  RID left_rid;
  RID right_rid;*/

  // cache tuples in vector
  /*while (left_executor_->Next(&left_tuple, &left_rid)) {
    left_tuples_.push_back(left_tuple);
  }
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    right_tuples_.push_back(right_tuple);
  }
  // record left and right position
  left_pos_ = left_tuples_.cbegin();
  right_pos_ = right_tuples_.cbegin();
  is_find_ = false;*/
  RID left_rid;
  left_ret_ = left_executor_->Next(&left_tuple_, &left_rid);
  is_find_ = false;
}

// use two vector and two iterator is not elegent
// the code is redundant
/*auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_pos_ != left_tuples_.cend()) {
    while (right_pos_ != right_tuples_.cend()) {
      Value is_true = plan_->predicate_->EvaluateJoin(&(*left_pos_), left_executor_->GetOutputSchema(), &(*right_pos_),
                                                      right_executor_->GetOutputSchema());
      if (is_true.GetAs<bool>()) {
        *tuple = InnerJoinTuple(&(*left_pos_), &(*right_pos_));
        *rid = tuple->GetRid();
        ++right_pos_;
        is_find_ = true;
        return true;
      }
      ++right_pos_;
    }
    if (!is_find_ && plan_->GetJoinType() == JoinType::LEFT) {
      *tuple = LeftJoinTuple(&(*left_pos_));
      *rid = tuple->GetRid();
      ++left_pos_;
      right_pos_ = right_tuples_.cbegin();
      is_find_ = false;
      return true;
    }
    ++left_pos_;
    right_pos_ = right_tuples_.cbegin();
    is_find_ = false;
  }
  return false;
}*/

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    if (!left_ret_) {
      return false;
    }
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (!is_find_ && plan_->GetJoinType() == JoinType::LEFT) {
        *tuple = LeftJoinTuple(&left_tuple_);
        *rid = tuple->GetRid();
        // set is_find is very important!!
        is_find_ = true;
        return true;
      }
      // change left_tuple to next and init
      RID left_rid;  // have no use
      left_ret_ = left_executor_->Next(&left_tuple_, &left_rid);
      right_executor_->Init();
      is_find_ = false;
      continue;
    }

    Value is_true = plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &(right_tuple),
                                                    right_executor_->GetOutputSchema());
    if (is_true.GetAs<bool>()) {
      is_find_ = true;
      *tuple = InnerJoinTuple(&left_tuple_, &right_tuple);
      *rid = tuple->GetRid();
      return true;
    }
  }
}

auto NestedLoopJoinExecutor::InnerJoinTuple(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple {
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

auto NestedLoopJoinExecutor::LeftJoinTuple(const Tuple *left_tuple) -> Tuple {
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
