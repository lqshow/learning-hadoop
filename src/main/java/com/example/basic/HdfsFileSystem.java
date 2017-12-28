package com.example.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Reference:
 * - http://mund-consulting.com/Blog/file-operations-in-hdfs-using-java/
 */
public class HdfsFileSystem {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        // The conf directory is set to ResourceRoot, can be directly debugged
        FileSystem fs = FileSystem.get(conf);

        Path workingDir = getWorkingDirectory(fs);
        getHomeDirectory(fs);
        mkdirs(fs, "new_folder/folder");

        // Merge paths
        Path newFolderPath = Path.mergePaths(workingDir, new Path("/new_folder/folder"));
        System.out.println("newFolderPath = " + newFolderPath);

        // Copying File from local file system to HDFS
        copyFromLocalFile(fs, "src/main/resources/daily_show_guests", "new_folder/folder");

        // Copying File from HDFS to local file system
        copyToLocalFile(fs, "/Users/linqiong/Downloads/daily_show_guests", "output/daily_show_guests");

        // Create new File
        fs.createNewFile(new Path("new_folder/folder/newFile"));

        // Delete folder
        rmr(fs, "new_folder");

        // List the contents specified folder
        list(fs, "output");
    }

    /**
     * Get Working directory
     *
     * @param fs
     * @return
     */
    static Path getWorkingDirectory(FileSystem fs) {
        Path workingDir = fs.getWorkingDirectory();
        System.out.println("workingDir = " + workingDir);
        /**
         * output
         *
         * workingDir = hdfs://localhost:9000/user/linqiong
         */

        return workingDir;
    }

    /**
     * Get home directory
     *
     * @param fs
     * @return
     */
    static Path getHomeDirectory(FileSystem fs) {
        Path homeDir = fs.getHomeDirectory();
        System.out.println("homeDir = " + homeDir);
        /**
         * output
         *
         * homeDir = hdfs://localhost:9000/user/linqiong
         */

        return homeDir;
    }

    /**
     * Create Directory
     *
     * @param fs
     * @param newFolderPath
     */
    static void mkdirs(FileSystem fs, String newFolderPath) {
        Path path = new Path(newFolderPath);
        try {
            if (fs.exists(path)) {
                // Delete existing Directory
                fs.delete(path, true);
            }
            // Create new Directory
            fs.mkdirs(path);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * copying File from local file system to HDFS
     *
     * @param fs
     * @param local
     * @param remote
     * @throws IOException
     */
    static void copyFromLocalFile(FileSystem fs, String local, String remote) throws IOException {
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
    }

    /**
     * Copying File from HDFS to local file system
     *
     * @param fs
     */
    static void copyToLocalFile(FileSystem fs, String local, String remote) throws IOException {
        fs.copyToLocalFile(new Path(remote), new Path(local));
        System.out.println("copy from: " + remote + " to " + local);
    }

    /**
     * Delete Directory/File
     *
     * @param fs
     * @param folder
     * @throws IOException
     */
    static boolean rmr(FileSystem fs, String folder) throws IOException {
        Path path = new Path(folder);

        System.out.println("Delete: " + folder);

        return fs.deleteOnExit(path);
    }

    /**
     * List the contents that match the specified file pattern
     * @param fs
     * @param folder
     * @throws IOException
     */
    static void list(FileSystem fs, String folder) throws IOException {
        Path path = new Path(folder);

        FileStatus fss = fs.getFileStatus(path);
        if (fss.isDirectory()) {
            FileStatus[] list = fs.listStatus(path);

            for (FileStatus f : list) {
                System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDirectory(), f.getLen());
            }
        }
    }

}
