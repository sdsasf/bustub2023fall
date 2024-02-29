//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"
namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
  if (max_depth > HTABLE_HEADER_MAX_DEPTH) {
    throw Exception("header page max_depth is too big!");
  }
  max_depth_ = max_depth;
  // if page id == 0, the page is not allocate
  for (auto i = 0; i < (1 << max_depth_); ++i) {
    directory_page_ids_[i] = 0;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  if (!max_depth_) {
    return 0;
  }
  uint32_t mask = ~((static_cast<uint32_t>(1) << (8 * HTABLE_HEADER_PAGE_METADATA_SIZE - max_depth_)) - 1);
  return (hash & mask) >> (8 * HTABLE_HEADER_PAGE_METADATA_SIZE - max_depth_);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

}  // namespace bustub
