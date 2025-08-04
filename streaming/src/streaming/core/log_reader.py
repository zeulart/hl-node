import os
import mmap
import time
import asyncio
from typing import Generator, Optional, Tuple
from pathlib import Path
import structlog

logger = structlog.get_logger(__name__)


class MemoryMappedLogReader:
    def __init__(self, file_path: str, buffer_size: int = 65536):
        self.file_path = Path(file_path)
        self.buffer_size = buffer_size
        self.file_handle: Optional[int] = None
        self.mmap_obj: Optional[mmap.mmap] = None
        self.current_offset = 0
        self.file_size = 0
        self.inode = None
        
    async def open(self) -> bool:
        try:
            if not self.file_path.exists():
                logger.warning("Log file does not exist", path=str(self.file_path))
                return False
                
            # Open file descriptor
            self.file_handle = os.open(str(self.file_path), os.O_RDONLY)
            
            # Get file info
            stat_info = os.fstat(self.file_handle)
            self.file_size = stat_info.st_size
            self.inode = stat_info.st_ino
            
            if self.file_size == 0:
                logger.info("File is empty, waiting for data", path=str(self.file_path))
                return True
                
            # Create memory map
            self.mmap_obj = mmap.mmap(
                self.file_handle, 
                0,  # Map entire file
                access=mmap.ACCESS_READ
            )
            
            logger.info(
                "Memory-mapped log file opened",
                path=str(self.file_path),
                size=self.file_size,
                inode=self.inode
            )
            return True
            
        except Exception as e:
            logger.error("Failed to open log file", path=str(self.file_path), error=str(e))
            await self.close()
            return False
    
    async def close(self):
        if self.mmap_obj:
            self.mmap_obj.close()
            self.mmap_obj = None
            
        if self.file_handle:
            os.close(self.file_handle)
            self.file_handle = None
            
        self.current_offset = 0
        self.file_size = 0
        self.inode = None
    
    def detect_rotation(self) -> bool:
        try:
            if not self.file_path.exists():
                return True
                
            stat_info = self.file_path.stat()
            
            # File was truncated or replaced (new inode)
            if stat_info.st_ino != self.inode or stat_info.st_size < self.current_offset:
                logger.info(
                    "Log rotation detected",
                    old_inode=self.inode,
                    new_inode=stat_info.st_ino,
                    old_size=self.file_size,
                    new_size=stat_info.st_size,
                    current_offset=self.current_offset
                )
                return True
                
            return False
            
        except Exception as e:
            logger.error("Error checking file rotation", error=str(e))
            return True
    
    def has_new_data(self) -> bool:
        try:
            if not self.file_path.exists():
                return False
                
            current_size = self.file_path.stat().st_size
            return current_size > self.file_size
            
        except Exception:
            return False
    
    async def refresh_mmap(self) -> bool:
        try:
            # Close existing mapping
            if self.mmap_obj:
                self.mmap_obj.close()
                self.mmap_obj = None
            
            # Get new file size
            stat_info = os.fstat(self.file_handle)
            self.file_size = stat_info.st_size
            
            if self.file_size == 0:
                return True
                
            # Create new memory map
            self.mmap_obj = mmap.mmap(
                self.file_handle,
                0,
                access=mmap.ACCESS_READ
            )
            
            return True
            
        except Exception as e:
            logger.error("Failed to refresh memory map", error=str(e))
            return False
    
    def read_new_lines(self) -> Generator[memoryview, None, None]:
        if not self.mmap_obj or self.file_size == 0:
            return
            
        start_offset = self.current_offset
        
        # Find all newline positions from current offset
        search_offset = start_offset
        while search_offset < self.file_size:
            # Find next newline
            newline_pos = self.mmap_obj.find(b'\n', search_offset)
            
            if newline_pos == -1:
                # No more complete lines
                break
                
            # Extract line (excluding newline)
            if newline_pos > search_offset:
                line_view = memoryview(self.mmap_obj)[search_offset:newline_pos]
                yield line_view
                
            # Move to next line
            search_offset = newline_pos + 1
        
        # Update current offset
        self.current_offset = search_offset
    
    async def read_lines_batch(self, max_lines: int = 1000) -> list[bytes]:
        lines = []
        count = 0
        
        for line_view in self.read_new_lines():
            if count >= max_lines:
                break
                
            # Convert memoryview to bytes for Redis
            lines.append(bytes(line_view))
            count += 1
            
        return lines
    
    def get_stats(self) -> dict:
        return {
            "file_path": str(self.file_path),
            "file_size": self.file_size,
            "current_offset": self.current_offset,
            "bytes_remaining": max(0, self.file_size - self.current_offset),
            "inode": self.inode,
            "is_mapped": self.mmap_obj is not None
        }


class LogReaderPool:
    def __init__(self):
        self.readers: dict[str, MemoryMappedLogReader] = {}
        
    async def add_reader(self, key: str, file_path: str) -> bool:
        reader = MemoryMappedLogReader(file_path)
        success = await reader.open()
        
        if success:
            self.readers[key] = reader
            logger.info("Added log reader", key=key, path=file_path)
        else:
            logger.error("Failed to add log reader", key=key, path=file_path)
            
        return success
    
    async def remove_reader(self, key: str):
        if key in self.readers:
            await self.readers[key].close()
            del self.readers[key]
            logger.info("Removed log reader", key=key)
    
    def get_reader(self, key: str) -> Optional[MemoryMappedLogReader]:
        return self.readers.get(key)
    
    async def close_all(self):
        for reader in self.readers.values():
            await reader.close()
        self.readers.clear()
        logger.info("Closed all log readers")
    
    def get_all_stats(self) -> dict:
        return {key: reader.get_stats() for key, reader in self.readers.items()}