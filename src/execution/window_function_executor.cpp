#include "execution/executors/window_function_executor.h"
#include <algorithm>
#include <iterator>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/*
auto WindowFunctionExecutor::Equal(Tuple *a, Tuple *b,
                                   const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys)
    -> bool {
  for (auto &order_by_pair : order_bys) {
    if (order_by_pair.second->Evaluate(a, child_executor_->GetOutputSchema())
            .CompareEquals(order_by_pair.second->Evaluate(b, child_executor_->GetOutputSchema())) != CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}*/

auto WindowFunctionExecutor::Equal(Tuple *a, Tuple *b,
                                   const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys)
    -> bool {
  auto pred = [&a, &b, schema = child_executor_->GetOutputSchema()](
                  const std::pair<OrderByType, AbstractExpressionRef> &pair) -> bool {
    return static_cast<bool>(pair.second->Evaluate(a, schema).CompareEquals(pair.second->Evaluate(b, schema)));
  };
  return std::all_of(order_bys.begin(), order_bys.end(), pred);
}

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  // throw NotImplementedException("WindowFunctionExecutor is not implemented");
  child_executor_->Init();
  tuples_.clear();

  Tuple child_tuple;
  RID child_rid;
  std::vector<Tuple> child_tuples;

  // store all tuple in a local vector
  // may use tuples multipy times, must cache in a local vector !!
  // child_tuples only store child_executor's tuples and sort
  // tuples_ in window executor only store result tuple, may be modify multiple times
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    child_tuples.push_back(child_tuple);
  }

  // internal comparator
  auto internal_comparator = [](const Tuple &left_tuple, const Tuple &right_tuple,
                                const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys,
                                const Schema &schema) -> bool {
    for (const auto &order_pair : order_bys) {
      switch (order_pair.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (order_pair.second->Evaluate(&left_tuple, schema)
                  .CompareLessThan(order_pair.second->Evaluate(&right_tuple, schema)) == CmpBool::CmpTrue) {
            return true;
          } else if (order_pair.second->Evaluate(&left_tuple, schema)
                         .CompareGreaterThan(order_pair.second->Evaluate(&right_tuple, schema)) == CmpBool::CmpTrue) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (order_pair.second->Evaluate(&left_tuple, schema)
                  .CompareGreaterThan(order_pair.second->Evaluate(&right_tuple, schema)) == CmpBool::CmpTrue) {
            return true;
          } else if (order_pair.second->Evaluate(&left_tuple, schema)
                         .CompareLessThan(order_pair.second->Evaluate(&right_tuple, schema)) == CmpBool::CmpTrue) {
            return false;
          }
      }
    }
    // if order_by is empty, don't change vector
    return false;
  };

  // record func column idx in a set
  std::unordered_set<uint32_t> func_column_idx_set;
  for (auto &[func_column_idx, _] : plan_->window_functions_) {
    func_column_idx_set.insert(func_column_idx);
  }

  // main loop, handle every window function struct
  for (auto &[curr_column_idx, window_func] : plan_->window_functions_) {
    // part order(group) and total order(if group tied then order by)
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> global_orders;
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> part_orders;
    for (const auto &partition_by : window_func.partition_by_) {
      global_orders.emplace_back(OrderByType::ASC, partition_by);
      part_orders.emplace_back(OrderByType::ASC, partition_by);
    }
    for (const auto &order_by : window_func.order_by_) {
      global_orders.push_back(order_by);
    }
    // sort tuples based on partition by and order by
    std::sort(child_tuples.begin(), child_tuples.end(),
              [&global_orders, schema = child_executor_->GetOutputSchema(), internal_comparator](
                  const Tuple &left_tuple, const Tuple &right_tuple) -> bool {
                return internal_comparator(left_tuple, right_tuple, global_orders, schema);
              });

    // child tuples have been grouped and sorted
    auto begin_iter = child_tuples.begin();
    while (begin_iter != child_tuples.end()) {
      auto upper_bound_iter =
          std::upper_bound(begin_iter, child_tuples.end(), *begin_iter,
                           [&part_orders, schema = child_executor_->GetOutputSchema(), internal_comparator](
                               const Tuple &left_tuple, const Tuple &right_tuple) -> bool {
                             return internal_comparator(left_tuple, right_tuple, part_orders, schema);
                           });
      // calculate aggregate function in a partition
      Value aggregate_value;
      Value default_value;
      // assign default value based on window func type
      switch (window_func.type_) {
        case WindowFunctionType::CountStarAggregate:
          aggregate_value = ValueFactory::GetIntegerValue(0);
          break;
        case WindowFunctionType::CountAggregate:
        case WindowFunctionType::SumAggregate:
        case WindowFunctionType::MinAggregate:
        case WindowFunctionType::MaxAggregate:
        case WindowFunctionType::Rank:
          aggregate_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
          break;
      }
      default_value = aggregate_value;

      // if not rank
      if (window_func.type_ != WindowFunctionType::Rank) {
        // if don't have order by clause
        if (window_func.order_by_.empty()) {
          for (auto iter = begin_iter; iter != upper_bound_iter; ++iter) {
            Value input_value = window_func.function_->Evaluate(&(*iter), child_executor_->GetOutputSchema());
            InsertCombin(window_func.type_, aggregate_value, input_value);
          }
        }
        for (auto iter = begin_iter; iter != upper_bound_iter; ++iter) {
          // res_tuple_iter(tuples_) point to the same position as iter(child_tuples)
          auto res_tuple_iter = tuples_.begin() + std::distance(child_tuples.begin(), iter);
          if (!window_func.order_by_.empty()) {
            Value input_value = window_func.function_->Evaluate(&(*iter), child_executor_->GetOutputSchema());
            InsertCombin(window_func.type_, aggregate_value, input_value);
          }

          // have got aggregate value
          std::vector<Value> values;
          /*
           * If it is this window function column, the value is the aggregation value
           * If it is not a window function column, the value should be from the child tuple
           * If it is another window function column and tuple have insert into tuples_, keep it
           * Else, the tuple haven't been inserted, can't use GetValue(will segment fault!!) let's populate a default
           * value
           *
           */
          for (uint32_t i = 0; i < GetOutputSchema().GetColumnCount(); ++i) {
            if (i == curr_column_idx) {
              values.push_back(aggregate_value);
            } else if (func_column_idx_set.count(i) == 0) {
              values.push_back(plan_->columns_[i]->Evaluate(&(*iter), child_executor_->GetOutputSchema()));
            } else if (res_tuple_iter != tuples_.end()) {
              values.push_back(res_tuple_iter->GetValue(&GetOutputSchema(), i));
            } else {
              values.push_back(default_value);
            }
          }
          if (res_tuple_iter != tuples_.end()) {
            *res_tuple_iter = Tuple(values, &GetOutputSchema());
          } else {
            tuples_.emplace_back(values, &GetOutputSchema());
          }
        }
        begin_iter = upper_bound_iter;
      } else {
        uint32_t local_rank = 0;
        uint32_t global_rank = 0;
        for (auto iter = begin_iter; iter != upper_bound_iter; ++iter) {
          auto res_tuple_iter = tuples_.begin() + std::distance(child_tuples.begin(), iter);
          std::vector<Value> values;
          for (uint32_t i = 0; i < GetOutputSchema().GetColumnCount(); ++i) {
            if (i == curr_column_idx) {
              ++global_rank;
              // if it is first group, the iter-1 will segment fault, so must add local_rank == 0U condition
              // !Equal(iter, iter-1) means change group
              if (local_rank == 0U || !Equal(&(*iter), &(*(iter - 1)), window_func.order_by_)) {
                local_rank = global_rank;
              }
              values.push_back(ValueFactory::GetIntegerValue(local_rank));
            } else if (func_column_idx_set.count(i) == 0) {
              values.push_back(plan_->columns_[i]->Evaluate(&(*iter), child_executor_->GetOutputSchema()));
            } else if (res_tuple_iter != tuples_.end()) {
              values.push_back(res_tuple_iter->GetValue(&GetOutputSchema(), i));
            } else {
              values.push_back(default_value);
            }
          }
          if (res_tuple_iter != tuples_.end()) {
            *res_tuple_iter = Tuple(values, &GetOutputSchema());
          } else {
            tuples_.emplace_back(values, &GetOutputSchema());
          }
        }
        begin_iter = upper_bound_iter;
      }
    }
  }
  tuple_iter_ = tuples_.begin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuple_iter_ == tuples_.end()) {
    return false;
  }
  *tuple = *tuple_iter_;
  *rid = tuple->GetRid();
  ++tuple_iter_;
  return true;
}

// I think it's not necessary to creat a hash table
// if not use hash table in aggregate executor, I have to rewrite the combine function
void WindowFunctionExecutor::InsertCombin(WindowFunctionType window_func_type, Value &aggregate_value,
                                          Value &input_value) {
  switch (window_func_type) {
    case WindowFunctionType::CountStarAggregate:
      aggregate_value = aggregate_value.Add(ValueFactory::GetIntegerValue(1));
      break;
    case WindowFunctionType::CountAggregate:
      if (aggregate_value.IsNull()) {
        aggregate_value = ValueFactory::GetIntegerValue(0);
      }
      aggregate_value = aggregate_value.Add(ValueFactory::GetIntegerValue(1));
      break;
    case WindowFunctionType::SumAggregate:
      if (aggregate_value.IsNull()) {
        aggregate_value = ValueFactory::GetIntegerValue(0);
      }
      aggregate_value = aggregate_value.Add(input_value);
      break;
    case WindowFunctionType::MinAggregate:
      if (aggregate_value.IsNull()) {
        aggregate_value = input_value;
      } else {
        aggregate_value = aggregate_value.Min(input_value);
      }
      break;
    case WindowFunctionType::MaxAggregate:
      if (aggregate_value.IsNull()) {
        aggregate_value = input_value;
      } else {
        aggregate_value = aggregate_value.Max(input_value);
      }
      break;
    default:
      break;
  }
}

}  // namespace bustub
