#pragma once

#include <iterator>
#include <list>
#include <unordered_map>

#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * @brief tracks all the read timestamps.
 *
 */
class Watermark {
 public:
  explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
   * correctly. */
  auto UpdateCommitTs(timestamp_t commit_ts) { commit_ts_ = commit_ts; }

  auto GetWatermark() -> timestamp_t {
    if (current_reads_.empty()) {
      // std::cout << "current_reads_ is empty " << watermark_ << "  " << commit_ts_ << std::endl;
      return commit_ts_;
    }
    return watermark_;
  }

  timestamp_t commit_ts_;

  timestamp_t watermark_;

  // insert all running read ts in unordered_map
  std::unordered_map<timestamp_t, int> current_reads_;
  // std::unordered_map<timestamp_t, std::iterator<list>> current_reads_;

  // The inserted read ts must be incremental
  // ts incremental list
  std::list<timestamp_t> current_reads_list_;
};

};  // namespace bustub
