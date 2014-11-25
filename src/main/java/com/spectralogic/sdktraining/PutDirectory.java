package com.spectralogic.sdktraining;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SignatureException;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.Ds3ClientBuilder;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers;
import com.spectralogic.ds3client.helpers.Ds3ClientHelpers.Job;
import com.spectralogic.ds3client.helpers.FileObjectPutter;
import com.spectralogic.ds3client.models.Credentials;
import com.spectralogic.ds3client.models.bulk.Ds3Object;
import com.spectralogic.ds3client.serializer.XmlProcessingException;

public class PutDirectory {

    public static void main(final String[] args) throws SignatureException, IOException, XmlProcessingException {
        final Ds3Client client = Ds3ClientBuilder.create("192.168.56.101:8080", new Credentials("c3BlY3RyYQ==", "m7AsqEnq"))
        		.withHttps(false) // Turn off SSL
        		.build();
        
        final Ds3ClientHelpers helpers = Ds3ClientHelpers.wrap(client);
        
        final String bucketName = "testBucket";        
        
        final Path pathToArchive = Paths.get("src/main/resources/books");

        archive(helpers, bucketName, pathToArchive);
        archive(helpers, "differentBucket", Paths.get("something/path"));
        
        System.out.println("Finished writing objects to Black Pearl");
    }
    
    private static void archive(final Ds3ClientHelpers helper, final String bucketName, final Path path) throws SignatureException, IOException, XmlProcessingException {
        // Need a call to make sure the bucket we are going to put data to exists
        helper.ensureBucketExists(bucketName);
        
        // Get the list of all the files in a directory
        
        final Iterable<Ds3Object> objects = helper.listObjectsForDirectory(path);
        
        System.out.println("Starting bulk put job");
        
        // Start the bulk job and put all the objects to black pearl
        
        Job job = helper.startWriteJob(bucketName, objects);
        
        job.transfer(new FileObjectPutter(path));    	
    }
}
