import socket


def get_network_ip():
    """获取本机地址,会获取首个网络地址"""
    _, _, ipaddrlist = socket.gethostbyname_ex(socket.gethostname())
    return ipaddrlist[0]


def ensure_host(host):
    return host or get_network_ip()