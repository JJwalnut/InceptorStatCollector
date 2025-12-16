package io.transwarp;

import java.util.concurrent.*;

/**
 * @Auther: jianjie
 * @Date: 2025/6/28 - 06 - 28 - 17:25
 * @Description: io.transwarp
 * @version: 1.0
 */
public class ProducerConsumerDemo {
    // 共享阻塞队列（容量10）
    private static BlockingQueue<String> queue = new LinkedBlockingQueue<>(10);

    // 生产者类
    static class Producer implements Runnable {
        private final String name;

        public Producer(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < 5; i++) {
                    // 生产数据并放入队列（自动处理队列满的情况）
                    String data = "数据-" + name + "-" + i;
                    queue.put(data);
                    System.out.println("生产者 " + name + " 生产：" + data);
                    TimeUnit.MILLISECONDS.sleep(500); // 模拟生产耗时
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // 消费者类
    static class Consumer implements Runnable {
        private final String name;

        public Consumer(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    // 从队列获取数据（自动处理队列空的情况）
                    String data = queue.take();
                    System.out.println("消费者 " + name + " 消费：" + data);
                    TimeUnit.MILLISECONDS.sleep(800); // 模拟消费耗时
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(8); // 8个线程（3生产者+5消费者）

        // 启动3个生产者
        executor.execute(new Producer("A"));
        executor.execute(new Producer("B"));
        executor.execute(new Producer("C"));

        // 启动5个消费者
        executor.execute(new Consumer("X"));
        executor.execute(new Consumer("Y"));
        executor.execute(new Consumer("Z"));
        executor.execute(new Consumer("M"));
        executor.execute(new Consumer("N"));

        // 等待一段时间后关闭线程池
        TimeUnit.SECONDS.sleep(10);
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        System.out.println("程序结束");
    }
}