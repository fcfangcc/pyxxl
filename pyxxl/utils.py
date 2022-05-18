import socket

from typing import Optional


def get_network_ip() -> str:
    """获取本机地址,会获取首个网络地址"""
    _, _, ipaddrlist = socket.gethostbyname_ex(socket.gethostname())
    return ipaddrlist[0]


def ensure_host(host: Optional[str] = None) -> str:
    return host or get_network_ip()
