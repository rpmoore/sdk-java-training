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
        final Ds3Client client = Ds3ClientBuilder.create("ds3_endpoint", new Credentials("access_key", "secret_key"))
        		.withHttps(false) // Turn off SSL
        		.build();
        
        final Ds3ClientHelpers helpers = Ds3ClientHelpers.wrap(client);
        
        final String bucketName = "testBucket";
        
        // Need a call to make sure the bucket we are going to put data to exists
        
        final Path pathToArchive = Paths.get("src/main/resources/books");
        
        // Get the list of all the files in a directory
        
        System.out.println("Starting bulk put job");
        
        // Start the bulk job and put all the objects to black pearl
        
        System.out.println("Finished writing objects to Black Pearl");
    }
}
