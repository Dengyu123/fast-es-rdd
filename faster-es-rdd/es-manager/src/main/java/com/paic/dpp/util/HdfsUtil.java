package com.paic.dpp.util;

import com.paic.dpp.factory.PojoFactory;
import com.paic.dpp.pojo.HdpConf;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/02
 */
public class HdfsUtil {
    private static Logger logger = LoggerFactory.getLogger(HdfsUtil.class);
    public static Configuration getConf(){
        Configuration conf = new Configuration();
        HdpConf hdpConf = (HdpConf) PojoFactory.getServiceBean("hdpConf");
        conf.set("fs.defaultFS",hdpConf.getDefaultFs());
        return conf;
    }

    public static String download2LocalWithoutPrefix(String file,Configuration conf) throws IOException {
        String s = download2Local(file,conf);
        return s.replaceAll("file://","");
    }
    /**
     * download hdfs file 2 local
     * @param file
     * @return
     * @throws IOException
     */
    public static String download2Local(String file,Configuration conf) throws IOException {
        if(file.startsWith("hdfs")){
            FileSystem fs = FileSystem.get(conf);
            Path src = new Path(file);
            FSDataInputStream inputStream = fs.open(src);
            String localPath = file.substring(file.lastIndexOf("/")+1);
            FileOutputStream os=new FileOutputStream(localPath);
            IOUtils.copy(inputStream,os);
            File localFile = new File(localPath);
            logger.info("Download hdfs file : ["+file+"] to local: ["+localFile.getAbsolutePath()+"]");
            return  "file://"+localFile.getAbsolutePath();
        }else if(file.startsWith("file")){
            return file;
        }else
            return "file://"+file;
    }

    /**
     * delete dir
     * @param conf
     * @param dir
     * @return
     */
    public static boolean deleteDir(Configuration conf,String dir){
        logger.info("Delete dir: "+dir);
        FileSystem fs = null;
        boolean res = false;
        try {
            fs = FileSystem.newInstance(conf);
            res = fs.delete(new Path(dir), true);
        } catch (Exception e) {
            logger.error("Dir delete error!dir:{}", dir);
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    /**
     * mkdir
     * @param dir
     * @param conf
     * @return
     */
    public static boolean mkdir(String dir, Configuration conf) {
        FileSystem fs = null;
        boolean res = false;
        try {
            Path srcPath = new Path(dir);
            fs = srcPath.getFileSystem(conf);
            res = fs.mkdirs(srcPath);
        } catch (Exception e) {
            logger.error("Mkdir error!dir:{}", dir);
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    /**
     * copy local dir to hdfs
     * @param src
     * @param dst
     * @param conf
     * @return
     * @throws Exception
     */
    public static boolean copyDirectory(String src, String dst, Configuration conf) throws Exception {
        Path path = new Path(dst);
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        File file = new File(src);
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            if (f.isDirectory()) {
                String fname = f.getName();
                if (dst.endsWith("/")) {
                    copyDirectory(f.getPath(), dst + fname + "/", conf);
                } else {
                    copyDirectory(f.getPath(), dst + "/" + fname + "/", conf);
                }
            } else {
                uploadFile(f.getPath(), dst, conf);
            }
        }
        return true;
    }

    /**
     * upload file to hdfs
     * @param localSrc
     * @param dst
     * @param conf
     * @return
     * @throws Exception
     */
    public static boolean uploadFile(String localSrc, String dst, Configuration conf) throws Exception {
        try {
            File file = new File(localSrc);
            dst = dst + "/" + file.getName();
            Path path = new Path(dst);
            FileSystem fs = path.getFileSystem(conf);
            fs.exists(path);
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            OutputStream out = fs.create(new Path(dst));
            org.apache.hadoop.io.IOUtils.copyBytes(in, out, 8092, true);
            in.close();
        } catch (Exception e) {
            logger.error("Copy file error!local:" + localSrc, e);
            throw e;
        }
        return true;
    }

}
