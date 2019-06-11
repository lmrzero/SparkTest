package cn.edu360.flume;

import java.io.*;
import java.net.Socket;
import java.util.Date;

/**
 * created by LMR on 2019/6/4
 */
public class DataProducer {

    public static void main(String[] args) throws Exception{
        new Thread(() -> {
            try {
                Socket socket = new Socket("master",44444);
                while (true)
                {
                    try {
                        socket.getOutputStream().write((new Date() + ": hello world").getBytes());
                        socket.getOutputStream().flush();
                        Thread.sleep(200000);
                        System.out.println("try");
                    }catch (Exception e)
                    {

                    }
                }
            }catch (IOException e)
            {

            }
        }).start();
    }
}
