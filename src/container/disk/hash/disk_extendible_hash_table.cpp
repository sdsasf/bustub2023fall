//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  BasicPageGuard header_guard = bpm->NewPageGuarded(&header_page_id_);
  WritePageGuard h_w_guard = header_guard.UpgradeWrite();
  auto header_page = h_w_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  V value;
  uint32_t hash = Hash(key);
  ReadPageGuard h_r_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = h_r_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t d_idx = header_page->HashToDirectoryIndex(hash);
  auto d_page_id = header_page->GetDirectoryPageId(d_idx);
  // always allocate header page first, so page id 0 is not available in directory and bucket page
  // so I use page id == 0 to indicate empty
  if (d_page_id != INVALID_PAGE_ID) {
    // std::cout << "Find directory page " << d_page_id << std::endl;
    ReadPageGuard d_r_guard = bpm_->FetchPageRead(d_page_id);
    auto d_page = d_r_guard.As<ExtendibleHTableDirectoryPage>();
    uint32_t b_idx = d_page->HashToBucketIndex(hash);
    // std::cout << "Find bucket idx " << b_idx << std::endl;
    auto b_page_id = d_page->GetBucketPageId(b_idx);
    if (b_page_id != INVALID_PAGE_ID) {
      // std::cout << "Find bucket page " << b_page_id << std::endl;
      ReadPageGuard b_r_guard = bpm_->FetchPageRead(b_page_id);
      auto b_page = b_r_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
      if (b_page->Lookup(key, value, cmp_)) {
        // std::cout << "Find key" << std::endl;

        result->push_back(value);
        return true;
      }
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  V v;
  uint32_t hash = Hash(key);
  // As should follow fetch right behind
  // Because As modify is_dirty in guard
  // is_dirty is saved in every page guard, determine if it should write back to disk during unpin()
  // unpin() modify is_dirty is page struct
  WritePageGuard h_w_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = h_w_guard.AsMut<ExtendibleHTableHeaderPage>();
  // if there is no free page in bpm, guard.page_ is nullptr(confuse me for a week !!!!!!!!)
  // As use page_->GetData directly !!!!
  if (header_page == nullptr) {
    // this shouldn't happen !!
    // std::cout << "There is no free space in bpm, can't fetch header page!" << std::endl;
    return false;
  }
  uint32_t d_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t d_page_id = header_page->GetDirectoryPageId(d_idx);
  if (d_page_id != INVALID_PAGE_ID) {
    h_w_guard.Drop();  // after find directory, drop header guard
    // fetch directory page
    WritePageGuard d_w_guard = bpm_->FetchPageWrite(d_page_id);
    auto d_page = d_w_guard.AsMut<ExtendibleHTableDirectoryPage>();
    if (d_page == nullptr) {
      // this shouldn't happen !!
      // std::cout << "There is no free space in bpm, can't fetch directory page!" << std::endl;
      return false;
    }
    // test
    // std::cout << "after find d_page_id " << d_page_id << std::endl;

    uint32_t b_idx = d_page->HashToBucketIndex(hash);
    page_id_t b_page_id = d_page->GetBucketPageId(b_idx);
    if (b_page_id != INVALID_PAGE_ID) {
      WritePageGuard b_w_guard = bpm_->FetchPageWrite(b_page_id);
      auto b_page = b_w_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      if (!b_page) {
        // this shouldn't happen !!
        // std::cout << "There is no free space in bpm, can't fetch bucket page!" << std::endl;
        return false;
      }
      // std::cout << "after find b_page_id " << b_page_id << std::endl;
      if (!(b_page->Lookup(key, v, cmp_))) {
        if (b_page->Insert(key, value, cmp_)) {
          // b_page->PrintBucket();
          // std::cout << "Insert key " << key << " to page " << b_page_id << std::endl;
          return true;
        }
        if (b_page->IsFull()) {  // if bucket overflow
          while (true) {
            if (d_page->GetGlobalDepth() == d_page->GetLocalDepth(b_idx)) {
              // if local depth equal to global depth
              if (d_page->GetGlobalDepth() < directory_max_depth_) {
                d_page->IncrGlobalDepth();
                // std::cout << "global depth " << d_page->GetGlobalDepth() << std::endl;
              } else {
                return false;
              }
            }
            // both need bucket split
            // allocate a new bucket page
            page_id_t image_b_page_id;
            BasicPageGuard image_b_page_guard = bpm_->NewPageGuarded(&image_b_page_id);
            if (image_b_page_guard.IsEmpty()) {  // no free page in bufferpool
              std::cout << "There is no free space in bpm, can't allocate image bucket page!" << std::endl;
              return false;
            }
            WritePageGuard image_b_w_page_guard = image_b_page_guard.UpgradeWrite();  // get write lock befor write
            auto image_b_page = image_b_w_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
            image_b_page->Init(bucket_max_size_);  // Init first

            // Update directory map first
            // cache old local depth and mask
            uint32_t local_depth = d_page->GetLocalDepth(b_idx);
            uint32_t local_depth_mask = d_page->GetLocalDepthMask(b_idx);
            uint32_t high_bit = 1 << local_depth;
            uint32_t image_b_idx = d_page->GetSplitImageIndex(b_idx);
            for (uint32_t i = (b_idx & local_depth_mask); i < d_page->Size(); i += high_bit) {
              if ((i & high_bit) == (image_b_idx & high_bit)) {
                d_page->SetBucketPageId(i, image_b_page_id);
              }
              // image bucket page and bucket page local depth + 1
              d_page->SetLocalDepth(i, local_depth + 1);
            }
            // finish update directory mapping
            // copy all entries in b_page
            std::list<std::pair<K, V>> entries;
            for (uint32_t i = 0; i < b_page->Size(); ++i) {
              entries.push_back(b_page->EntryAt(i));
            }
            b_page->Clear();
            for (auto entry : entries) {
              uint32_t target_idx = d_page->HashToBucketIndex(Hash(entry.first));
              page_id_t target_page_id = d_page->GetBucketPageId(target_idx);
              assert(target_page_id == b_page_id || target_page_id == image_b_page_id);
              if (target_page_id == b_page_id) {
                b_page->Insert(entry.first, entry.second, cmp_);
              } else if (target_page_id == image_b_page_id) {
                image_b_page->Insert(entry.first, entry.second, cmp_);
              }
            }
            // std::cout << "------------------------------" << std::endl;
            // std::cout << "After split page " << std::endl;
            // for (uint32_t i = 0; i < d_page->Size(); ++i) {
            //   std::cout << i << "   " << d_page->GetBucketPageId(i) << " local depth " << d_page->GetLocalDepth(i)
            //             << std::endl;
            // }
            // std::cout << "------------------------------" << std::endl;
            // std::cout << "new local depth " << d_page->GetLocalDepth(b_idx) <<std::endl;
            // drop bucket and image bucket
            image_b_w_page_guard.Drop();
            b_w_guard.Drop();
            // try insert after bucket split, may be fail again !!!
            b_idx = d_page->HashToBucketIndex(hash);     // global depth may be increase
            b_page_id = d_page->GetBucketPageId(b_idx);  // get new bucket
            b_w_guard = bpm_->FetchPageWrite(b_page_id);
            b_page = b_w_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
            if (!b_page) {
              // this shouldn't happen !!
              std::cout << "There is no free space in bpm, can't fetch bucket page!" << std::endl;
              return false;
            }
            if (b_page->Insert(key, value, cmp_)) {
              // std::cout << "Insert key " << key << " in page " << b_page_id << std::endl;
              return true;
            }
          }
        }
      }
    } else {
      if (InsertToNewBucket(d_page, b_idx, key, value)) {
        // std::cout << "Insert new bucket idx = " << b_idx << std::endl;
        return true;
      }
    }
  } else {
    if (InsertToNewDirectory(header_page, d_idx, hash, key, value)) {
      // std::cout << "Insert new directory idx = " << d_idx << std::endl;
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t d_page_id;
  BasicPageGuard new_d_page_guard = bpm_->NewPageGuarded(&d_page_id);
  if (new_d_page_guard.IsEmpty()) {  // no free page in bufferpool
    return false;
  }
  auto new_d_w_page_guard = new_d_page_guard.UpgradeWrite();  // get write lock befor write
  auto new_d_page = new_d_w_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  new_d_page->Init(directory_max_depth_);  // Init first
  if (this->InsertToNewBucket(new_d_page, hash & new_d_page->GetGlobalDepthMask(), key, value)) {
    header->SetDirectoryPageId(directory_idx, d_page_id);  // Insert into header page
    // std::cout << "Insert new directory idx = " << directory_idx << std::endl;
    return true;
  }
  // If insert failed, delete allocated directory page
  bpm_->DeletePage(d_page_id);
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t b_page_id;
  BasicPageGuard new_b_page_guard = bpm_->NewPageGuarded(&b_page_id);
  if (new_b_page_guard.IsEmpty()) {  // no free page in bufferpool
    return false;
  }
  auto new_b_w_page_guard = new_b_page_guard.UpgradeWrite();  // get write lock befor write
  auto new_b_page = new_b_w_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_b_page->Init(bucket_max_size_);
  new_b_page->Insert(key, value, cmp_);  // Insert into empty bucket should not fail

  directory->SetBucketPageId(bucket_idx, b_page_id);
  directory->SetLocalDepth(bucket_idx, 0);  // new bucket local depth equal to 0 ??
  // only for test
  // new_b_page->PrintBucket();

  // std::cout << "Insert new bucket idx = " << bucket_idx << std::endl;
  // std::cout << "Insert key " << key << " to page " << directory->GetBucketPageId(bucket_idx) << std::endl;
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  // assume new local depth has increased 1 !!
  // assume new bucket_idx is (bucket_idx & NewLocalDepthMask)!! could use GetSplitImage directly
  uint32_t high_bit = 1 << (new_local_depth - 1);
  uint32_t new_local_depth_mask = (1 << new_local_depth) - 1;
  for (uint32_t i = (new_bucket_idx & local_depth_mask); i < directory->Size(); i += high_bit) {
    if ((i & new_local_depth_mask) == (new_bucket_idx & new_local_depth_mask)) {
      directory->SetBucketPageId(i, new_bucket_page_id);
    }
    directory->SetLocalDepth(i, new_local_depth);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  ReadPageGuard h_r_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = h_r_guard.As<ExtendibleHTableHeaderPage>();  // use read guard first
  uint32_t d_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t d_page_id = header_page->GetDirectoryPageId(d_idx);
  h_r_guard.Drop();  // early release
  if (d_page_id != INVALID_PAGE_ID) {
    WritePageGuard d_w_guard = bpm_->FetchPageWrite(d_page_id);  // directory is always need to write
    auto d_page = d_w_guard.AsMut<ExtendibleHTableDirectoryPage>();
    // std::cout << "Find directory page " << d_page_id << std::endl;
    uint32_t b_idx = d_page->HashToBucketIndex(hash);
    page_id_t b_page_id = d_page->GetBucketPageId(b_idx);
    if (b_page_id != INVALID_PAGE_ID) {
      WritePageGuard b_w_guard = bpm_->FetchPageWrite(b_page_id);
      auto b_page = b_w_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      // std::cout << "Find bucket page " << b_page_id << std::endl;
      if (b_page->Remove(key, cmp_)) {
        // std::cout << "remove key " << key << " in page " << b_page_id << std::endl;
        if (b_page->IsEmpty()) {  // if bucket is empty after remove
          while (true) {
            // only local depth not equal to 0, it has split image !!
            if (d_page->GetLocalDepth(b_idx) == 0U) {
              break;
            }
            // reverse highest bit that is image idx
            uint32_t image_idx = b_idx ^ (static_cast<uint32_t>(1) << (d_page->GetLocalDepth(b_idx) - 1));
            if (d_page->GetLocalDepth(b_idx) != d_page->GetLocalDepth(image_idx)) {
              // if local depth is not equal, can't merge
              break;
            }
            // if have image page, fetch image page
            uint32_t image_page_id = d_page->GetBucketPageId(image_idx);
            WritePageGuard image_page_guard = bpm_->FetchPageWrite(image_page_id);
            auto image_page = image_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
            if (!image_page->IsEmpty() && !b_page->IsEmpty()) {
              break;
            }
            // migrate entry to b_page
            for (uint32_t i = 0; i < image_page->Size(); ++i) {
              std::pair<K, V> entry = image_page->EntryAt(i);
              b_page->Insert(entry.first, entry.second, cmp_);
            }
            image_page->Clear();
            image_page_guard.Drop();
            // update mapping
            uint32_t new_local_depth_mask = d_page->GetLocalDepthMask(b_idx) >> 1;
            uint32_t new_high_bit = 1 << (d_page->GetLocalDepth(b_idx) - 1);
            // idx point to either b_page or image page, update to point to image page and decrease local depth
            for (uint32_t i = (b_idx & new_local_depth_mask); i < d_page->Size(); i += new_high_bit) {
              d_page->SetBucketPageId(i, b_page_id);
              // decrease every local depth that related
              d_page->DecrLocalDepth(i);
            }
          }
          while (d_page->CanShrink()) {
            d_page->DecrGlobalDepth();
          }
          /*
          std::cout << "------------------------------" << std::endl;
          std::cout << "After merge page " << std::endl;
          for (uint32_t i = 0; i < d_page->Size(); ++i) {
            std::cout << i << "   " << d_page->GetBucketPageId(i) << " local depth " << d_page->GetLocalDepth(i)
                      << std::endl;
          }
          std::cout << "------------------------------" << std::endl;
          */
        }
        // b_page->PrintBucket();
        return true;
      }
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx,
                                                       uint32_t local_depth_mask) {  // note overflow
  uint32_t new_local_depth_mask = (local_depth_mask << 1) + 1;
  uint32_t sz = old_bucket->Size();
  // remove array entry directly, the index i will invalid
  for (uint32_t i = 0; i < sz; ++i) {
    if ((Hash(old_bucket->KeyAt(i)) & new_local_depth_mask) == (new_bucket_idx & new_local_depth_mask)) {
      new_bucket->Insert(old_bucket->KeyAt(i), old_bucket->ValueAt(i), cmp_);
    }
  }
  for (uint32_t i = 0; i < sz; ++i) {
    if ((Hash(old_bucket->KeyAt(i)) & new_local_depth_mask) == (new_bucket_idx & new_local_depth_mask)) {
      old_bucket->RemoveAt(i);
    }
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
