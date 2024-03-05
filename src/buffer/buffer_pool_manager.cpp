//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  /* throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");*/

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t f_id;
  if (!(free_list_.empty())) {
    f_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (replacer_->Evict(&f_id)) {
      if (pages_[f_id].IsDirty()) {
        auto promise1 = disk_scheduler_->CreatePromise();
        auto future1 = promise1.get_future();
        disk_scheduler_->Schedule(
            {/*is_write=*/true, pages_[f_id].GetData(), /*page_id=*/pages_[f_id].page_id_, std::move(promise1)});
        if (!future1.get()) {
          throw Exception("write disk data error!");
        }
      }
      page_table_.erase(pages_[f_id].page_id_);
    } else {
      return nullptr;
    }
  }
  page_id_t p_id = AllocatePage();
  *page_id = p_id;
  pages_[f_id].ResetMemory();
  pages_[f_id].page_id_ = p_id;
  replacer_->RecordAccess(f_id);
  replacer_->SetEvictable(f_id, false);
  pages_[f_id].pin_count_ = 1;
  pages_[f_id].is_dirty_ = false;
  page_table_[p_id] = f_id;
  return &pages_[f_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  auto ptr = page_table_.find(page_id);
  frame_id_t f_id;
  if (ptr == page_table_.end()) {
    if (!(free_list_.empty())) {
      f_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (replacer_->Evict(&f_id)) {
        if (pages_[f_id].IsDirty()) {
          auto promise1 = disk_scheduler_->CreatePromise();
          auto future1 = promise1.get_future();
          disk_scheduler_->Schedule(
              {/*is_write=*/true, pages_[f_id].GetData(), /*page_id=*/pages_[f_id].page_id_, std::move(promise1)});
          if (!future1.get()) {
            throw Exception("write disk data error!");
          }
        }
        page_table_.erase(pages_[f_id].page_id_);
      } else {
        return nullptr;
      }
    }
    pages_[f_id].ResetMemory();
    pages_[f_id].page_id_ = page_id;
    pages_[f_id].pin_count_ = 0;
    pages_[f_id].is_dirty_ = false;
    page_table_[page_id] = f_id;
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule({/*is_write=*/false, pages_[f_id].GetData(), /*page_id=*/page_id, std::move(promise1)});
    if (!future1.get()) {
      throw Exception("write disk data error!");
    }
  } else {
    f_id = ptr->second;
  }
  replacer_->RecordAccess(f_id);
  replacer_->SetEvictable(f_id, false);
  ++pages_[f_id].pin_count_;
  // pages_[f_id].is_dirty_ = true;
  return &pages_[f_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  auto ptr = page_table_.find(page_id);
  if (ptr == nullptr || pages_[ptr->second].pin_count_ == 0) {
    return false;
  }
  if ((--pages_[ptr->second].pin_count_) == 0) {
    replacer_->SetEvictable(ptr->second, true);
  }
  if (is_dirty) {
    pages_[ptr->second].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::lock_guard<std::mutex> lk(latch_);
  auto ptr = page_table_.find(page_id);
  if (ptr == nullptr) {
    return false;
  }
  frame_id_t f_id = ptr->second;
  auto promise1 = disk_scheduler_->CreatePromise();
  auto future1 = promise1.get_future();
  disk_scheduler_->Schedule({/*is_write=*/true, pages_[f_id].GetData(), /*page_id=*/page_id, std::move(promise1)});
  if (!future1.get()) {
    throw Exception("write disk data error!");
  }
  pages_[f_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lk(latch_);
  for (auto &ptr : page_table_) {
    FlushPage(ptr.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  auto ptr = page_table_.find(page_id);
  if (ptr == nullptr) {
    return true;
  }
  if (pages_[ptr->second].pin_count_ > 0) {
    return false;
  }
  replacer_->Remove(ptr->second);
  free_list_.push_back(ptr->second);
  pages_[ptr->second].page_id_ = INVALID_PAGE_ID;
  page_table_.erase(ptr);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *p = FetchPage(page_id);
  if (p == nullptr) {
    throw Exception("can't fetch page");
  }
  return {this, p};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *p = FetchPage(page_id);
  if (p != nullptr) {
    p->RLatch();
  }
  // std::cout << "Fectch page read  " << page_id << " Pin count " << p->GetPinCount() << std::endl;
  return {this, p};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *p = FetchPage(page_id);
  if (p != nullptr) {
    p->WLatch();
  }
  // std::cout << "Fectch page write  " << page_id << " Pin count " << p->GetPinCount() << std::endl;
  return {this, p};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *p = NewPage(page_id);
  /*if (p != nullptr) {
    std::cout << "allocate new page " << *page_id << std::endl;
  }*/
  // if there is no free page, page guard page is nullptr
  return {this, p};
}

}  // namespace bustub
