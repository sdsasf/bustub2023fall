//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  bool have_less_k_access = false;
  frame_id_t fid;
  size_t max_timestamp = std::numeric_limits<size_t>::max();
  {
    std::lock_guard<std::mutex> lk(latch_);
    if (curr_size_ == 0) {
      return false;
    }
    for (auto &ptr : node_store_) {
      if (!ptr.second.IsEvictable()) {
        continue;
      }
      if (have_less_k_access) {
        if (ptr.second.AccessNum() >= k_) {
          continue;
        }
        if (ptr.second.History() < max_timestamp) {
          max_timestamp = ptr.second.History();
          fid = ptr.first;
        }
      } else {
        if (ptr.second.AccessNum() < k_) {
          have_less_k_access = true;
          max_timestamp = ptr.second.History();
          fid = ptr.first;
        } else {
          if (ptr.second.History() < max_timestamp) {
            max_timestamp = ptr.second.History();
            fid = ptr.first;
          }
        }
      }
    }
  }
  node_store_.erase(fid);
  --curr_size_;
  *frame_id = fid;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lk(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("Invalid frame id!");
  }
  auto ptr = node_store_.find(frame_id);
  if (ptr == node_store_.end()) {
    node_store_[frame_id] = LRUKNode(frame_id, k_, current_timestamp_);
  } else {
    ptr->second.Access(current_timestamp_);
  }
  ++current_timestamp_;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lk(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("Invalid frame id!");
  }
  if (node_store_[frame_id].IsEvictable() && !set_evictable) {
    --curr_size_;
    node_store_[frame_id].SetEvictable(set_evictable);
  } else if (!(node_store_[frame_id].IsEvictable()) && set_evictable) {
    ++curr_size_;
    node_store_[frame_id].SetEvictable(set_evictable);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(latch_);
  auto ptr = node_store_.find(frame_id);
  if (ptr == node_store_.end()) {
    return;
  }
  if (!(ptr->second.IsEvictable())) {
    throw Exception("Not find frame id!");
  }
  node_store_.erase(ptr);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lk(latch_);
  return curr_size_;
}

}  // namespace bustub
