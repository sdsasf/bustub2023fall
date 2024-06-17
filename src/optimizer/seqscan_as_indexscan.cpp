#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  // copy plan node with children
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    // optimize children first
    // children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    // if seq_scan plan has a predicate
    if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
        cmp_expr != nullptr) {
      // only optimize if expr is ComparisonType::Equal and there's only one equality test
      if (cmp_expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[0].get());
            column_value_expr != nullptr) {
          // find index info
          TableInfo *table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
          std::vector<IndexInfo *> indexes = catalog_.GetTableIndexes(table_info->name_);
          uint32_t column_idx = column_value_expr->GetColIdx();

          for (const IndexInfo *idx : indexes) {
            const std::vector<uint32_t> &key_attr = idx->index_->GetKeyAttrs();
            if (key_attr[0] == column_idx) {
              return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                         idx->index_oid_, seq_scan_plan.filter_predicate_);
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
