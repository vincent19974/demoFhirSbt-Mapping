package com.amazonaws.samples;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectResult;


public class S3Example {

	public static void main(String[] args) {
	
			
		AmazonS3 s3Obj = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
	
	//	
		
	// Create Bucket
		
	//	
		
	//	Bucket s3Bucket = s3Obj.createBucket("sasabuc2");
		
	//	System.out.println("My first s3 Java bucket: " + s3Bucket.getName());
		
	//
		
	// Copy file to Bucket
		
	//
		
	//	java.io.File fajl = new java.io.File ("/home/sjovanovic/Documents/Databar/proba1.hql");
		
	//	PutObjectResult objResult = s3Obj.putObject("sasabuc2", "proba1.hql", fajl);
			
	//	System.out.println("My first copy file to s3 bucket");
		
	//
		
	// Delete file from Bucket
		
	//
		
	// s3Obj.deleteObject("sasabuc2", "proba1.hql");
		
	
	// System.out.println("Delete file form the Bucket");
		
		
	}
	
}
