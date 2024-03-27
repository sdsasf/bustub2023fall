#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
namespace bustub {

auto DfsExpression(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_table_exprs,
                   std::vector<AbstractExpressionRef> &right_table_exprs) -> bool;

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  auto left_table_exprs = std::vector<AbstractExpressionRef>();
  auto right_table_exprs = std::vector<AbstractExpressionRef>();
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.predicate_) {
      // Has exactly two children
      BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
      if (DfsExpression(nlj_plan.Predicate(), left_table_exprs, right_table_exprs)) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), left_table_exprs, right_table_exprs,
                                                  nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

auto DfsExpression(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_table_exprs,
                   std::vector<AbstractExpressionRef> &right_table_exprs) -> bool {
  // Recursive termination condition
  if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); cmp_expr != nullptr) {
    if (cmp_expr->comp_type_ == ComparisonType::Equal) {
      if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
          left_expr != nullptr) {
        if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
            right_expr != nullptr) {
          auto left_expr_tuple = std::make_shared<ColumnValueExpression>(
              left_expr->GetTupleIdx(), left_expr->GetColIdx(), left_expr->GetReturnType());
          auto right_expr_tuple = std::make_shared<ColumnValueExpression>(
              right_expr->GetTupleIdx(), right_expr->GetColIdx(), right_expr->GetReturnType());

          if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
            left_table_exprs.push_back(std::move(left_expr_tuple));
            right_table_exprs.push_back(std::move(right_expr_tuple));
            return true;
          }
          if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
            left_table_exprs.push_back(std::move(right_expr_tuple));
            right_table_exprs.push_back(std::move(left_expr_tuple));
            return true;
          }
        }
      }
    }
    return false;
  }
  if (const auto *logic_expr = dynamic_cast<LogicExpression *>(expr.get()); logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      if (DfsExpression(logic_expr->children_[0], left_table_exprs, right_table_exprs) &&
          DfsExpression(logic_expr->children_[1], left_table_exprs, right_table_exprs)) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
