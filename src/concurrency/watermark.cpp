#include "concurrency/watermark.h"
#include <exception>
#include <shared_mutex>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  std::unique_lock<std::shared_mutex> lck(latch_);
  // TODO(fall2023): implement me!
  if (current_reads_.count(read_ts) != 0U) {
    ++current_reads_[read_ts];
  } else {
    // if current_reads is empty, must modify watermark_ first
    if (current_reads_.empty()) {
      watermark_ = read_ts;
    }
    // current_reads_list_.push_back(read_ts);
    current_reads_[read_ts] = 1;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  std::unique_lock<std::shared_mutex> lck(latch_);
  if (current_reads_[read_ts] == 1) {
    current_reads_.erase(read_ts);
    /*
    for (auto i = current_reads_list_.begin(); i != current_reads_list_.end(); ++i) {
      if (*i == read_ts) {
        current_reads_list_.erase(i);
        break;
      }
    }
    */
  } else {
    --current_reads_[read_ts];
  }

  if (read_ts == watermark_ && !current_reads_.empty()) {
    // watermark_ = current_reads_list_.front();
    watermark_ = current_reads_.begin()->first;
  }
}

}  // namespace bustub
