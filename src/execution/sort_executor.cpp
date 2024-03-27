#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // throw NotImplementedException("SortExecutor is not implemented");
  child_executor_->Init();
  tuples_.clear();

  Tuple temp_tuple;
  RID temp_rid;
  while (child_executor_->Next(&temp_tuple, &temp_rid)) {
    tuples_.push_back(temp_tuple);
  }
  auto &order_by = plan_->GetOrderBy();
  auto comparator = [order_bys = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](
                        Tuple &left_tuple, Tuple &right_tuple) -> bool {
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
    return false;
  };

  std::sort(tuples_.begin(), tuples_.end(), comparator);
  tuple_iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuple_iter_ == tuples_.end()) {
    return false;
  }
  *tuple = *tuple_iter_;
  *rid = tuple->GetRid();
  ++tuple_iter_;
  return true;
}

}  // namespace bustub
