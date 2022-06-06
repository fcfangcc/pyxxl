# xxl-jobs 的python客户端实现

<p align="center">
<a href="https://pypi.org/project/pyxxl" target="_blank">
    <img src="https://img.shields.io/pypi/v/pyxxl?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/pyxxl" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/pyxxl.svg?color=%2334D058" alt="Supported Python versions">
</a>
<a href="https://pypi.org/project/pyxxl" target="_blank">
    <img src="https://img.shields.io/codecov/c/github/fcfangcc/pyxxl?color=%2334D058" alt="Coverage">
</a>
</p>

使用pyxxl可以方便的把Python写的方法注册到xxl-job中,使用xxl-job-admin管理Python定时任务和周期任务

## 如何使用

```shell
pip install pyxxl
```

使用示例 [example](https://fcfangcc.github.io/pyxxl/example/) ，具体代码在example文件夹下面


## 开发人员
下面是开发人员如何快捷的搭建开发调试环境

### 启动xxl的调度中心

```shell
./init_dev_env.sh
```


### 启动执行器


```shell
poetry install
# 修改app.py中相关的配置信息,然后启动
poetry run python example/app.py
```



## TODOs
- [x] 自定义查看日志函数
- [-] docs
