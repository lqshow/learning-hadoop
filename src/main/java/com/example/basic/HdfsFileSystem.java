package com.example.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Reference:
 * - http://mund-consulting.com/Blog/file-operations-in-hdfs-using-java/
 */
public class HdfsFileSystem {
    private static Configuration conf;

    public static void main(String[] args) throws IOException {
        conf = new Configuration();

        // Change fs.defaultFs
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        // The conf directory is set to ResourceRoot, can be directly debugged
        FileSystem fs = FileSystem.get(conf);

        Path workingDir = getWorkingDirectory(fs);
        getHomeDirectory(fs);

        // Merge paths
        Path newFolderPath = Path.mergePaths(workingDir, new Path("/new_folder/folder"));
        System.out.println("newFolderPath = " + newFolderPath);


        // Create new File
        touchz("new_folder/folder/newFile");

        // Create Directory
        mkdirs("new_folder/folder");

        // Copying File from local file system to HDFS
        copyFromLocalFile("src/main/resources/file/daily_show_guests", "new_folder/folder");

        // Move files
        mv("new_folder/folder/daily_show_guests", "new_folder/folder/daily_show_guests_rename");

        // Copying File from HDFS to local file system
        copyToLocalFile("/Users/linqiong/Downloads/daily_show_guests", "output/daily_show_guests");

        // Delete folder
        rmr("new_folder");

        // List the contents specified folder
        ls("output");
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
     * @param newFolderPath
     */
    static boolean mkdirs(String newFolderPath) {
        Path path = new Path(newFolderPath);
        try (FileSystem fs = FileSystem.get(conf)) {
            if (fs.exists(path)) {
                // Delete existing Directory
                fs.delete(path, true);
            }
            // Create new Directory
            return fs.mkdirs(path);
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * copying File from local file system to HDFS
     *
     * @param local
     * @param remote
     * @throws IOException
     */
    static void copyFromLocalFile(String local, String remote) {
        try (FileSystem fs = FileSystem.get(conf)) {
            fs.copyFromLocalFile(new Path(local), new Path(remote));
            System.out.println("copy from: " + local + " to " + remote);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Copying File from HDFS to local file system
     *
     * @param local
     * @param remote
     * @throws IOException
     */
    static void copyToLocalFile(String local, String remote) {
        try (FileSystem fs = FileSystem.get(conf)) {
            fs.copyToLocalFile(new Path(remote), new Path(local));
            System.out.println("copy from: " + remote + " to " + local);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Delete Directory/File
     *
     * @param folder
     * @throws IOException
     */
    static boolean rmr(String folder) {
        Path path = new Path(folder);

        try (FileSystem fs = FileSystem.get(conf)) {
            System.out.println("Delete: " + folder);

            return fs.deleteOnExit(path);
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * List the contents that match the specified file pattern
     *
     * @param folder
     * @throws IOException
     */
    static void ls(String folder) {
        Path path = new Path(folder);

        try (FileSystem fs = FileSystem.get(conf)) {
            FileStatus fss = fs.getFileStatus(path);
            if (fss.isDirectory()) {
                FileStatus[] list = fs.listStatus(path);

                for (FileStatus f : list) {
                    long timeStamp = f.getModificationTime();
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String modification = format.format(timeStamp);

                    System.out.printf("path: %s, permission: %s, modification: %s, folder: %s, size: %d\n",
                            f.getPath(), f.getPermission(), modification,
                            f.isDirectory(), f.getLen());
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Check the path exists
     *
     * @param path
     * @return
     */
    static boolean test(String path) {
        try (FileSystem fs = FileSystem.get(conf)) {
            return fs.exists(new Path(path));
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * Creates a file of zero length at <path>
     * @param path
     */
    static boolean touchz(String path) {
        try (FileSystem fs = FileSystem.get(conf)) {
            return fs.createNewFile(new Path(path));
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * Move files
     * @param srcPath
     * @param dstPath
     * @return
     */
    static boolean mv(String srcPath, String dstPath) {
        try (FileSystem fs = FileSystem.get(conf)) {
            return fs.rename(new Path(srcPath), new Path(dstPath));
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }
}
