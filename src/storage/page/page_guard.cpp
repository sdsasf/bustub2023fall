#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    // std::cout << "Unpin page " << page_->GetPageId() << " Pin count " << page_->GetPinCount() << std::endl;
  }
  bpm_ = nullptr;
  page_ = nullptr;
  // if no free page, the page is nullptr while bpm is not
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // Solve self-assignment
  if (this == &that) {
    return *this;
  }
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

auto BasicPageGuard::IsEmpty() -> bool { return page_ == nullptr; }

BasicPageGuard::~BasicPageGuard() { this->Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (page_ != nullptr) {
    page_->RLatch();
  }
  auto read_page_guard = ReadPageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return read_page_guard;
  /*ReadPageGuard r_guard(this->bpm_, this->page_);
  // r_guard.guard_.is_dirty_ = is_dirty_;
  bpm_ = nullptr;
  page_ = nullptr;
  return r_guard;*/
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (page_ != nullptr) {
    page_->WLatch();
  }
  auto write_page_guard = WritePageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return write_page_guard;
  /*WritePageGuard w_guard(this->bpm_, this->page_);
  // r_guard.guard_.is_dirty_ = is_dirty_;
  bpm_ = nullptr;
  page_ = nullptr;
  return w_guard;*/
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    if (guard_.page_ != nullptr) {
      guard_.page_->RUnlatch();
    }
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    // std::cout << "Read page guard drop page " << guard_.page_->GetPageId() << std::endl;
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

auto ReadPageGuard::IsEmpty() -> bool { return guard_.IsEmpty(); }

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    // std::cout << "Write page guard drop page " << guard_.page_->GetPageId() << std::endl;
    // is necessary ??
    // guard_.bpm_->FlushPage(guard_.page_->GetPageId());
    guard_.page_->WUnlatch();
  }
  // upgrade write can't modify is_dirty
  // so this line is necessary
  guard_.is_dirty_ = true;
  guard_.Drop();
}

auto WritePageGuard::IsEmpty() -> bool { return guard_.IsEmpty(); }

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
