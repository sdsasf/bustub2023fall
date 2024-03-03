#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  that.bpm_ = nullptr;
  page_ = that.page_;
  that.page_ = nullptr;
  is_dirty_ = that.is_dirty_;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    if (!(bpm_->UnpinPage(page_->GetPageId(), is_dirty_))) {
      throw Exception("unpin page error!");
    }
    std::cout << "Unpin page " << page_->GetPageId() << " Pin count " << page_->GetPinCount() << std::endl;
  }
  bpm_ = nullptr;
  page_ = nullptr;
  // if no free page, the page is nullptr while bpm is not
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->Drop();
  bpm_ = that.bpm_;
  that.bpm_ = nullptr;
  page_ = that.page_;
  that.page_ = nullptr;
  is_dirty_ = that.is_dirty_;
  return *this;
}

auto BasicPageGuard::IsEmpty() -> bool { return page_ == nullptr; }

BasicPageGuard::~BasicPageGuard() { this->Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard r_guard(this->bpm_, this->page_);
  // r_guard.guard_.is_dirty_ = is_dirty_;
  bpm_ = nullptr;
  page_ = nullptr;
  return r_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard w_guard(this->bpm_, this->page_);
  // r_guard.guard_.is_dirty_ = is_dirty_;
  bpm_ = nullptr;
  page_ = nullptr;
  return w_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->Drop();
  guard_ = std::move(that.guard_);
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
  this->Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    // std::cout << "Write page guard drop page " << guard_.page_->GetPageId() << std::endl;
    // is necessary ??
    // guard_.bpm_->FlushPage(guard_.page_->GetPageId());
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

auto WritePageGuard::IsEmpty() -> bool { return guard_.IsEmpty(); }

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
