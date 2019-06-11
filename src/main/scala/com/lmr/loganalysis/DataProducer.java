package com.lmr.loganalysis;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;

/**
 * created by LMR on 2019/6/5
 */
public class DataProducer {//建立一个服务端，使用Socket通信，向固定端口发送消息，模拟日志产生

    public static String[] site={
            "www.sougou.com",
            "www.baidu.com",
            "www.google.com",
            "www.wangyi.com",
            "www.youdao.com",
            "www.tengxun.com"

    };
    public static String[] ip = {
            "192.168.177.11",
            "12.252.12.25",
            "78.2.35.3",
            "12,35.23.65",
            "55.33.33.65",
            "123.56.32.255"
    };
    public static String[]  browsers = {
            "Google",
            "QQ",
            "UC",
            "Baidu",
            "IE",
            "Fixfox"
    };

    public static int getIndex(int length){
        Random random = new Random();
        return random.nextInt(length);
    }
    public static void main(String[] args){

        new Thread(() -> {
            try {
                Socket socket = new Socket("master",44444);
                String str="";
                while (true)
                {
                    str = site[getIndex(site.length)]+" "+browsers[getIndex(browsers.length)]+" "+getIndex(10)+"\n";
                    try {
                        socket.getOutputStream().write(str.getBytes());
                        socket.getOutputStream().flush();
                        Thread.sleep(10000);
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
