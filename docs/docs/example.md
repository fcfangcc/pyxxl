# Example And Deploy

## Examples

For full examples see [github](https://github.com/fcfangcc/pyxxl/tree/main/example).

## Run Executor App

```python
{!../../example/executor_app.py!}
```

```bash
python3 executor_app.py
```

## Run With Gunicorn Server
!!! note
    由于Gunicorn一般是多进程模式部署，所以executor不能在webapp中绑定启动

    需要在Gunicorn启动之后（when_ready）单独起一个进程来运行executor

    此部署方式适用于任何可以被Gunicorn部署的web框架集成PYXXL

app.py

```python
{!../../example/gunicorn_app/app.py!}
```

gunicorn.conf.py
```python
{!../../example/gunicorn_app/gunicorn.conf.py!}
```

```bash
gunicorn -c gunicorn.conf.py app:app -b 0.0.0.0:9000
```

## Run with Flask (Only for develop)


!!! Warning
    此案例仅用于开发模式和本地调试使用，部署到生产环境时强烈建议和webapp分开部署！

    由于Flask一般是多进程模式部署，和executor一起部署时需要executor需要单独启动，不能绑定在flask_app上

    如果确定要部署在一起，参考gunicorn的案例


```python
{!../../example/flask_app.py!}
```
