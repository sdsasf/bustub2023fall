#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // throw NotImplementedException("TopNExecutor is not implemented");
  // init must return to init state, so all the state must clear !!
  child_executor_->Init();
  tuples_.clear();

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
          break;
      }
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comparator)> heap(comparator);
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    heap.push(child_tuple);
    if (heap.size() > plan_->n_) {
      heap.pop();
    }
  }
  uint32_t sz = heap.size();
  for (uint32_t i = 0; i < sz; ++i) {
    tuples_.push_back(heap.top());
    heap.pop();
  }
  tuple_iter_ = tuples_.rbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuple_iter_ == tuples_.rend()) {
    return false;
  }
  *tuple = *tuple_iter_;
  *rid = tuple->GetRid();
  ++tuple_iter_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // throw NotImplementedException("TopNExecutor is not implemented");
  return tuples_.size();
};
}  // namespace bustub
