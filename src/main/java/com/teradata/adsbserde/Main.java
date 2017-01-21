/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.adsbserde;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * A driver to test the conversion algorithm.
 * 
 * <p>
 * Accepts a raw ADSB file name from the command line then processes it through
 * the conversion method.
 * </p>
 * 
 * @author Glenn McCall.
 */
public class Main {
    
    public static void main(String[] args) {
        Main m = new Main();
        m.go(args);
    }
    
    public void go(String [] args) {
        
        if (args.length == 0) {
            System.out.println("Please specify at least one file name on the command line.");
            System.exit(0);
        }
        
        for (String fileName : args) {
            System.out.println("file: " + fileName);
            process(fileName);
        }
    }
    
    
    public void process(String fileName) {
        AdsbSerDe serDe = new AdsbSerDe();
        
        LinkedList<String> colNames = new LinkedList();
        colNames.add("clock");
        colNames.add("airground");
        colNames.add("hexid");
        colNames.add("alt");
        colNames.add("heading");
        colNames.add("ident");
        colNames.add("lat");
        colNames.add("lon");
        colNames.add("speed");
        colNames.add("squawk");
        
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(fileName));
            String inLine = null;
            while ((inLine = br.readLine()) != null) {
                inLine = inLine.trim();
                //System.out.println(inLine);
                List<Object> row = serDe.processRecord(inLine, colNames);
                
                boolean first = true;
                for (Object o : row) {
                    if (first) {
                        System.out.print(o);
                    } else {
                        System.out.print("," + o);
                    }
                    first = false;
                }
                System.out.println();
            }
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException openning: " + fileName);
            System.out.println(e.toString());
        } catch (IOException e) {
            System.out.println("IOException reading: " + fileName);
            System.out.println(e.toString());
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    ;
                }
            }
        }
        
    }
    
}
