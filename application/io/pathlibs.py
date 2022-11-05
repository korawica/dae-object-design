import os
import re
import shutil
import fnmatch
import datetime
from pathlib import (
    Path,
    PosixPath
)
from typing import (
    AnyStr,
    Optional,
    Iterator,
    Callable
)

join_os_sep: Callable = os.sep.join

replace_os: Callable = lambda path: path.replace(os.sep, '/')


def join_path(full_path: AnyStr, full_join_path: str, abs: bool = True) -> AnyStr:
    """Join path with multi pardir value if set `full_join_path` be '../../<path>'."""
    _abspath: AnyStr = full_path
    if re.search(r'^(\w+://)', _abspath) or (not abs):
        # Return joined value with '/' string value if the full path starts with protocol prefix.
        return '/'.join([replace_os(_abspath), replace_os(full_join_path)])
    _join_split: list = os.path.normpath(full_join_path).split(os.sep)
    for path in _join_split:
        _abspath: AnyStr = os.path.abspath(os.path.join(_abspath, os.pardir)) if path == '..' \
            else os.path.abspath(os.path.join(_abspath, path))
    return replace_os(_abspath)


def join_root_with(full_join_path: str, key_name: str = 'APP_PATH') -> AnyStr:
    """Join path with the root path which set in `key_name` environment variable."""
    if key_value := os.getenv(key_name):
        return join_path(key_value, full_join_path)
    raise ValueError(
        f"the key name: {key_name!r} of root path in environment variable does not exists"
    )


def get_modification_time(path: str):
    """Return datetime of modification of file."""
    timestamp = os.path.getmtime(path)
    return datetime.datetime.fromtimestamp(timestamp)


def get_files(path: str, pattern: str) -> Iterator[PosixPath]:
    """Return path from glob method."""
    yield from Path(path).glob(pattern)


def remove_file(path, is_dir: bool = False):
    """param <path> could either be relative or absolute.
    """
    if os.path.isfile(path) or os.path.islink(path):
        # remove the file
        os.remove(path)
    elif os.path.isdir(path) and is_dir:
        # remove dir and all contains
        shutil.rmtree(path)
    else:
        raise ValueError(f"file {path!r} is not a file{' or dir' if is_dir else ''}.")


def touch(filename, times=None):
    file_handle = open(filename, 'a')
    try:
        os.utime(filename, times)
    finally:
        file_handle.close()


class PathSearch:
    """Path Search object"""

    @classmethod
    def from_dict(cls, _dict: dict):
        """Return Path Search object with dictionary"""
        return cls(
            root=_dict['root'],
            exclude_folder=_dict.get('exclude_folder'),
            exclude_name=_dict.get('exclude_name'),
            max_level=_dict.get('max_level', -1),
            length=_dict.get('length', 4),
            icon=_dict.get('icon', 1),
        )

    def __init__(
            self,
            root: str,
            *,
            exclude_name: Optional[list] = None,
            exclude_folder: Optional[list] = None,
            max_level: int = -1,
            length: int = 4,
            icon: int = 1
    ):
        self.root: str = root
        self.exclude_folder: list = exclude_folder or []
        self.exclude_name: list = exclude_name or []
        self.max_level: int = max_level
        self.length: int = length
        self.real_level: int = 0
        self._icon_last: str = self.icons[icon]["last"]
        self._icon_next: str = self.icons[icon]["next"]
        self._icon: str = self.icons[icon]["normal"]
        self._icon_length: int = len(self._icon)
        assert (self._icon_length + 1) < self.length, 'a `length` argument must gather than length of icon.'

        self.output_buf: list = [f"[{self.root.rsplit(os.path.sep, 1)[-1]}]"]
        self.output_files: list = []
        try:
            self._recurse(self.root, os.listdir(self.root), "", 0)
        except FileNotFoundError:
            pass

    @property
    def level(self) -> int:
        """Return level of sub path from the root path."""
        return self.real_level + 1 if self.max_level == -1 else self.max_level

    @property
    def files(self) -> list:
        """Return files which include in the root path."""
        return self.output_files

    @property
    def icons(self) -> dict:
        return {
            1: {'normal': "│", 'next': "├─", 'last': "└─"},
            2: {'normal': "┃", 'next': "┣━", 'last': "┗━"}
        }

    def _recurse(self, parent_path: str, file_list: list, prefix: str, level: int):
        """Path recursive method for generate buffer of tree and files."""
        if not file_list or (self.max_level != -1 and self.max_level <= level):
            return

        self.real_level = max(level, self.real_level)
        file_list.sort(key=lambda f: os.path.isfile(join_path(parent_path, f)))
        for idx, sub_path in enumerate(file_list):
            if any(exclude_name in sub_path for exclude_name in self.exclude_name):
                continue

            full_path: str = join_path(parent_path, sub_path)
            idc = self._switch_icon(idx, len(file_list))

            if os.path.isdir(full_path) and sub_path not in self.exclude_folder:
                self.output_buf.append(f"{prefix}{idc}[{sub_path}]")
                tmp_prefix: str = (
                    f'{prefix}{self._icon}{" " * (self.length - self._icon_length)}'
                    if len(file_list) > 1 and idx != len(file_list) - 1
                    else f'{prefix}{" " * self.length}'
                )
                self._recurse(full_path, os.listdir(full_path), tmp_prefix, level + 1)
            elif os.path.isfile(full_path):
                self.output_buf.append(f"{prefix}{idc}{sub_path}")
                self.output_files.append(full_path)

    def pick(self, filename: str) -> list:
        """Return filename with match with input argument."""
        return list(filter(
            lambda file: fnmatch.fnmatch(file, f'*/{filename}'),
            self.output_files
        ))

    def make_tree(self, newline: Optional[str] = None) -> str:
        """Return path tree of root path."""
        _newline: str = newline or '\n'
        return _newline.join(self.output_buf)

    def _switch_icon(self, number_now: int, number_all: int):
        return self._icon_last if number_now == (number_all - 1) else self._icon_next


if __name__ == '__main__':
    touch('test.txt')
