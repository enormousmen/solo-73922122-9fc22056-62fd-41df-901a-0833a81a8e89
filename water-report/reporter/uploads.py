"""上传原件的落盘与清理。

全部是纯文件操作，调用方保证执行这些操作时**不持有任何数据库事务**，
因此文件写入/清理不会占住 SQLite 写锁，已有批次照常可查询、可报送。
"""
from __future__ import annotations

import hashlib
import os
import secrets
from pathlib import Path

INCOMING = "incoming"   # 上传目录下的暂存子目录（*.part）


def unique_stored_name(original_name: str) -> str:
    """为每次上传生成互不相同的原件名：同名并发上传各自保留，绝不覆盖。"""
    p = Path(original_name)
    stem = p.stem.strip() or "upload"
    suffix = p.suffix or ".csv"
    return f"{stem}-{secrets.token_hex(6)}{suffix}"


def part_path(upload_dir: Path, stored_name: str) -> Path:
    return upload_dir / INCOMING / f"{stored_name}.part"


def final_path(upload_dir: Path, stored_name: str) -> Path:
    return upload_dir / stored_name


def write_original(upload_dir: Path, stored_name: str,
                   content: bytes) -> Path:
    """先写 incoming/*.part（O_EXCL 防并发覆盖），fsync 后原子硬链接就位。

    就位用 os.link 而不是 os.replace：目标名已存在时抛出 FileExistsError，
    保证任何情况下都不会覆盖已有原件；链接是原子操作，就位名只会以
    完整内容出现，绝不会被读到写了一半的文件。
    """
    incoming = upload_dir / INCOMING
    incoming.mkdir(parents=True, exist_ok=True)
    part = part_path(upload_dir, stored_name)
    final = final_path(upload_dir, stored_name)
    fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.link(part, final)   # 原子就位；已存在则报错，绝不覆盖
        os.unlink(part)
        try:  # 尽力让目录项也落盘
            dfd = os.open(upload_dir, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
    except BaseException:
        try:
            os.unlink(part)
        except FileNotFoundError:
            pass
        raise
    return final


def delete_originals(upload_dir: Path, stored_name: str) -> None:
    """删除原件与可能残留的 .part（幂等；清理尽力而为，不阻断调用方）。"""
    for p in (part_path(upload_dir, stored_name),
              final_path(upload_dir, stored_name)):
        try:
            p.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
