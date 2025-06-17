# 如何使用Timer
在作者原本的基础上，我改成了tokio的设计思路

这个自定义的Timer类模仿了计时器的基本工作原理，在初始化时可以给计时器取一个名字，设置控制开关，并且设置触发间隔，以及下次触发的时间等属性。

首先，你需要创建一个定时器的示例：
```rust
use crate::raft::timer::Timer;
// ...
let mut my_timer = Timer::new("MyTimer");

```

之后可以设置定时器的触发间隔，并且提供一个回调函数，这个回调函数会在计时器触发的时候执行。

```rust
my_timer.schedule(Duration::from_secs(1), || {
    println!("Hello world! The timer has triggered!");
})
```
上述设置了定时器每1s触发一次，并在触发时打印一条消息。

如果需要的话，还可以在计时器运行的过程中重置它的触发间隔。

```rust
my_timer.resert(Duration::from_secs(2)); // 将计时器的触发间隔重置为2s
```

最终停止计时器。

当不再需要计时器时，可以调用stop方法来停止它。

```rust
my_timer.stop();
```

示例代码，可以做一个简单的定时打印：

```rust
use std::time::{Duration, Instant};
use crate::raft::timer::Timer;

async fn main() {
    let mut my_timer = Timer::new("test");
    my_timer.schedule(Duration::from_secs(1), ||{
        println!("Thsi message is printed every second!");
    });
    tokio::time::sleep(run_duration).await;

    my_timer.stop();

}
```


## 如何设计Timer
.. TODO

之后再详细介绍，简单来讲就是原来需要每次新建一个Timer示例都需要创建一个线程来维护， 现在使用tokio的事件循环机制，理论上来说效率会更高，先按下不表，之后再介绍。

