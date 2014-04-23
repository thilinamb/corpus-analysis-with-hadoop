package cs455.hadoop.util;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: Thilina
 * Date: 4/23/14
 */
public class GetDecadesFromFileNames {
    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            System.err.println("You need to mention the directory to get the list of files.");
            System.exit(-1);
        }

        String dirName = args[0];
        boolean century = "century".equals(args[2].toLowerCase()) ? true : false;
        File directory = new File(dirName);

        if (!directory.isDirectory()) {
            System.err.println(dirName + " is not a directory.");
            System.exit(-1);
        }

        String[] files = directory.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".txt");
            }
        });

        Set<Integer> decades = new HashSet<Integer>();

        for (String file : files) {
            int publishedYear = getPublishedYearFromFileName(file);
            if (!century) {
                publishedYear = publishedYear - (publishedYear % 10);
            } else {
                publishedYear = publishedYear - (publishedYear % 100);
            }
            decades.add(publishedYear);
        }

        try {
            writeOutput(decades, args[1]);
        } catch (IOException e) {
            System.err.println("Error writing output");
            System.exit(-1);
        }
    }

    public static int getPublishedYearFromFileName(String fileName) {
        String publishedYearStr = fileName.split("[0-9]*-Year")[1].split(".txt")[0];
        int publishedYear;
        if (publishedYearStr.toUpperCase().contains("BC")) {
            publishedYearStr = publishedYearStr.replaceFirst("BC", "");
            // For books published in BC, multiply the number by -1
            publishedYear = Integer.parseInt(publishedYearStr) * -1;
        } else {
            publishedYear = Integer.parseInt(publishedYearStr);
        }
        return publishedYear;
    }

    public static void writeOutput(Set<Integer> set, String fileName) throws IOException {
        FileWriter fileWriter = null;
        File outFile = null;
        try {
            outFile = new File(fileName);

            if (outFile.exists()) {
                outFile.renameTo(new File(fileName + ".old"));
            }

            boolean status = outFile.createNewFile();

            if (!status) {
                System.err.println("Error writing output");
                System.exit(-1);
            }

            fileWriter = new FileWriter(outFile);

            for (Integer decade : set) {
                String line = decade + "\n";
                fileWriter.write(line.toCharArray());
            }
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

}
