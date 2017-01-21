/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.adsbserde;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * A simple logger to log to a known location.
 * 
 * <p>
 * Mostly I used this because i could not find where the "official" logger
 * messages were ending up and I was wasting too much time to find them.
 * </p>
 * <p>
 * Note: this will always append to the existing log file - so be aware to turn
 * the logging off if after you have what you need after turning logging on.
 * </p>
 * 
 * @author Glenn McCall
 */
public class MyLogger {
    
    public static boolean enabled = false;
    
    public static String logFileName = "/tmp/adsbSerDe.log";
    
    public static void print(String str) {
        if (!enabled) {
            return;
        }
        
        BufferedWriter bw = null;
        PrintStream ps = null;
        try {
            ps = new PrintStream(new FileOutputStream(logFileName, true));
            ps.print(str);
        } catch (IOException e) {
            ;
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    ;
                }
            }
        }
    }
    
    public static void println(String str) {
        print(str + "\n");
    }
    
    public static void println() {
        print("\n");
    }
    
}
