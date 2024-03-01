//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  // throw NotImplementedException("ExtendibleHTableBucketPage not implemented");
  size_ = 0;
  max_size_ = max_size;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  // binary search
  if (size_ > 0) {
    uint32_t l = 0;
    uint32_t r = size_ - 1;
    while (l < r) {
      uint32_t mid = l + (r - l) / 2;
      if (cmp(KeyAt(mid), key) < 0) {
        l = mid + 1;
      } else {
        r = mid;
      }
    }
    if (!cmp(KeyAt(l), key)) {
      value = ValueAt(l);
      return true;
    }
    /* for (auto i = 0; i < size_; ++i) {
     if (!cmp(KeyAt(i), key)) {
       value = ValueAt(i);
       return true;
     }
   }*/
  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  if (this->IsFull()) {
    return false;
  }
  uint32_t l = 0;
  uint32_t r = size_ - 1;
  if (size_ > 0) {
    while (l < r) {
      uint32_t mid = l + (r - l) / 2;
      if (cmp(KeyAt(mid), key) < 0) {
        l = mid + 1;
      } else {
        r = mid;
      }
    }
    if (!cmp(KeyAt(l), key)) {
      return false;
    }
    if (cmp(KeyAt(l), key) < 0) {
      ++l;
    }
    for (auto j = size_ - 1; j >= l; --j) {
      array_[j + 1] = array_[j];
    }
  }
  array_[l] = std::make_pair(key, value);
  ++size_;
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  for (auto i = 0; i < size_; ++i) {
    if (!cmp(key, KeyAt(i))) {
      for (auto j = i + 1; j < size_; ++j) {
        array_[j - 1] = array_[j];
      }
      --size_;
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  // throw NotImplementedException("ExtendibleHTableBucketPage not implemented");
  for (auto i = bucket_idx + 1; i < size_; ++i) {
    array_[i - 1] = array_[i];
  }
  --size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return (size_ == max_size_);
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return (size_ == 0);
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
