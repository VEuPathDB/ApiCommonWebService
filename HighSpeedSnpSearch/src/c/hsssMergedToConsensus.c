#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h> 

// using global variables to cut down on stack pushing operations.

// input parameters
FILE *strainFile;
FILE *refFile;
int minMajorAllelePerTenThou;
int unknownsThreshold;
int strainCount;
char *sourceIdPrefix;

// values read from input strains file (and pointers to them)
int16_t seq = -1;
int32_t loc = -1;
char allele = -1;  
char product = -1;  
int8_t strain = -1;  

// values read from reference strains file (and pointers to them)
int16_t refSeq = 0;
int32_t refLoc = 0;
char refAllele; 
char refProduct;

// per SNP counters
int alleleCount;   // number of known alleles for this SNP (might include more than one per strain, if diploid)
int nonRefStrainsCount = 0;   // number of strains that are not like-reference in this SNP
int alleles[5] = {0};  // hold count of alleles.  init array to 0. 0th allele is unknowns

// hold count of products. initialize array to 0.  the 0th product element means no product.  the 27th is for nonsense product
int products[4][28] = {{0}, {0}, {0}, {0}};


// prev SNP
int prevProduct = -1;
int prevStrain = -1;
int16_t prevSeq;
int32_t prevLoc;

static char *stdoutStr = "STDOUT";

static inline void initProductArrays(int prodArray[4][28]) {
	int i;
	for (i=0; i<4; i++) { 
		int j;
		for (j=0; j<28; j++) prodArray[i][j] = 0;
	}
}

// in a product array, find the product with the highest count.
// also see if there is more than one with non-zero count (isVariable)
static inline void findMaxProduct(int allele, char *product, char *isVariable) {
	int max = 0;
	int i;
	int index = allele - 1;

	for (i=0; i<27; i++) {
		int prodCount = products[index][i];
		if (prodCount > max) {
			if (max != 0) *isVariable = 1;
			max = prodCount;
			*product = i + 64;
		}
	}
	if (products[index][27] > max) {
		*product = '*';
	}
}

static inline char findMaxAllele(char *maxAllele, int *maxCount) {
	int i;
	char isVariable = 0;
	for (i=1; i<5; i++) {
		int alleleCnt = alleles[i];
		if (alleleCnt > *maxCount) {
			if (*maxCount != 0) isVariable = 1;
			*maxAllele = i;
			*maxCount = alleleCnt;
		}
	}
	return isVariable;
}

// read binary input and check for error
static inline int freadCheck (char *filename, void *ptr, size_t size, size_t count, FILE *stream) {
	int bytes = fread(ptr, size, count,stream);
	if (ferror(stream)) {
		fprintf(stderr, "Failed reading file '%s' \n", filename );
	 	exit(-1);
	}
	return bytes;
}

// write binary output and check for error
static inline int fwriteCheck (char *filename, void *ptr, size_t size, size_t count, FILE *stream) {
	fwrite(ptr, size, count,stream);
	if (ferror(stream)) {
		fprintf(stderr, "Failed writing to file '%s' \n", filename );
	 	exit(-1);
	}
}

// read a row of merged strains input file (ie, a variant) and update global variables with the values
static inline int readStrainRow(char *filename) {
	prevSeq = seq;
	prevLoc = loc;
	if (product > 0) prevProduct = product;  // remember last known product
	prevStrain = strain;

	freadCheck(filename, &seq, 2, 1, strainFile);  
	freadCheck(filename, &loc, 4, 1, strainFile);  
	freadCheck(filename, &allele, 1, 1, strainFile); 
	freadCheck(filename, &product, 1, 1, strainFile);
	return freadCheck(filename, &strain, 2, 1, strainFile);
}

// update the current SNP's alleles and products counts with the current variant's info
static inline updateCounts() {

	// -1 product means the strain id is actually a compressed count of unknowns for this SNP
	if (allele == 0 && product == -1) {
		alleles[0] += strain;
		nonRefStrainsCount += strain;
	} else {
		alleles[allele]++;
		if (product != 0) {
			if (product == '*') products[allele-1][27]++;
			else products[allele-1][product-64]++;  // normalize for ascii A.  we want A to be a 1
		}
		alleleCount++;
		if (strain != prevStrain) nonRefStrainsCount++;
	}
}

static inline writeRecord(int16_t prevSeq, int32_t prevLoc, char majorAllele, char majorProduct, char majorProductIsVariable, char minorAllele, char minorProduct, char minorProductIsVariable, int16_t majorAllelePerTenThou, int16_t minorAllelePerTenThou, char isTriallelic) {

	//		fprintf(stderr, "%i\t%i\t%i\t%i\t%i\t%i\t%i\t%i\t%i\t%i\t%i\n", prevSeq, prevLoc, majorAllele, majorProduct, majorProductIsVariable, minorAllele, minorProduct, minorProductIsVariable, majorAllelePerTenThou, minorAllelePerTenThou, minorProductIsVariable);
			fwriteCheck(stdoutStr, &prevSeq, 2, 1, stdout);
			fwriteCheck(stdoutStr, &prevLoc, 4, 1, stdout);
			fwriteCheck(stdoutStr, &majorAllele, 1, 1, stdout);
			fwriteCheck(stdoutStr, &majorProduct, 1, 1, stdout);
			fwriteCheck(stdoutStr, &majorProductIsVariable, 1, 1, stdout);
			fwriteCheck(stdoutStr, &minorAllele, 1, 1, stdout);
			fwriteCheck(stdoutStr, &minorProduct, 1, 1, stdout);
			fwriteCheck(stdoutStr, &minorProductIsVariable, 1, 1, stdout);
			fwriteCheck(stdoutStr, &majorAllelePerTenThou, 2, 1, stdout);
			fwriteCheck(stdoutStr, &minorAllelePerTenThou, 2, 1, stdout);
			fwriteCheck(stdoutStr, &isTriallelic, 1, 1, stdout);
}

// read a row from the ref genome file. it is a strain file containing one row per SNP, showing the ref genome's values
static inline getRefGenomeInfo(char *filename, int16_t seq, int32_t loc) {

	// advance through reference SNPs to the one that corresponds to the input SNP we are processing
	while(1) {
		int16_t refSeq22;
		freadCheck(filename, &refSeq, 2, 1, refFile);  
		freadCheck(filename, &refLoc, 4, 1, refFile);  
		freadCheck(filename, &refAllele, 1, 1, refFile); 
		int bytes = freadCheck(filename, &refProduct, 1, 1, refFile);

		if (refAllele == 0) {
			fprintf(stderr, "Reference allele zero at SNP: %i %i\n", seq, loc);
			exit(-1);
		}

		if (bytes == 0 || (refSeq == seq && refLoc== loc)) break;

		// along the way, write out a record for SNPs that completely agree w/ reference 
		// (ie, that are not in our input SNPs)
		writeRecord(refSeq, refLoc, refAllele, refProduct, 0, 0, 0, 0, 10000, 0, 0);
	}
}

main(int argc, char *argv[]) {

	if ( argc != 6 ) {
		fprintf(stderr,"\nProduce a binary SNP consensus from an input of merged strain files.\n\nUsage: %s mergedStrainFiles refGenomeFile strainCount maxPolymorphismPct unknownsThreshold\n\nWhere:\n  strainCount: number of input strains\n  minMajorAllelePct:  there must be this percent or more major alleles for a SNP to be reported\n  unknownsThreshold: there must be this many or fewer unknowns for this SNP to be reported.\n\nBinary output: contig_id(2), location(4), major_allele(1), major_allele_product(1), major_product_is_variable(1), minor_allele(1), minor_allele_product(1), minor_product_is_variable(1), major_allele_perTenThou(2), minor_allele_perTenThou(2), triallelic(1)\n", argv[0] );
		return -1;
	}

	strainCount = atoi(argv[3]);
	minMajorAllelePerTenThou = atoi(argv[4]) * 100;
	unknownsThreshold = atoi(argv[5]);

	strainFile = fopen(argv[1], "rb");
	if (strainFile == 0) {
		fprintf(stderr, "Can't open strainFile '%s' \n", argv[1] );
		return -1;
	}

	refFile = fopen(argv[2], "rb");
	if (refFile == 0) {
		fprintf(stderr,  "Can't open refGenomeFile '%s' \n", argv[2] );
		return -1;
	}

	int strainFileGot;
	// prime things by reading, but not processing, first SNP in the input
	strainFileGot = readStrainRow(argv[1]);
	prevSeq = seq;
	prevLoc = loc;
	while (seq == prevSeq && loc == prevLoc && strainFileGot != 0) {
		updateCounts();
		strainFileGot = readStrainRow(argv[1]);		
	}

	// read and process the rest of the SNPs in the input
	while(strainFileGot != 0) {

		// first process prev SNP and clear counts
		// fprintf(stderr, "%i %i %i %i\n", seq, prevSeq, loc, prevLoc);
		if (seq != prevSeq || loc != prevLoc) processPreviousSnp(prevSeq, prevLoc, argv[2]);

		// update counts with this variant
		updateCounts();

		// read next variant
		strainFileGot = readStrainRow(argv[1]);
	}	
	processPreviousSnp(prevSeq, prevLoc, argv[2]); // process final snp
	getRefGenomeInfo(argv[2], 0, 0); // read and write trailing ref genome rows

	fclose(strainFile);
	fclose(refFile);
	return 0;
}

/* 
 * Look at the data accumulated for a SNP.  If above threshold write it out.
 * As part of this, read the refGenome file to convert absent variants to ref genome values.
 */
processPreviousSnp(int32_t prevSeq, int32_t prevLoc, char *refGenomeFileName) {
	// only consider SNPs that are under or equal to unknowns threshold
	if (alleles[0] <= unknownsThreshold) {

		// get reference genome allele and product for this SNP and add to counts
		getRefGenomeInfo(refGenomeFileName, prevSeq, prevLoc);

		int ref_count = strainCount - nonRefStrainsCount; // nonRefStrainsCount includes unknowns; diploid strains are only counted once
		alleleCount += ref_count;
		alleles[refAllele] += ref_count;

		if (refProduct != 0) {
			if (refProduct == '*') products[refAllele-1][27] += ref_count;
			else products[refAllele-1][refProduct-64] += ref_count;  // subtract 64 to make A=1
		}

		// find major allele
		char majorAllele;
		int majorCount = 0;
		findMaxAllele(&majorAllele, &majorCount);

		// find major product
		char majorProduct = 63;   // 63 = ascii(A) - 1 which will be converted to '-' hsssFindMajorAlleles.c
		char majorProductIsVariable = 0;
		findMaxProduct(majorAllele, &majorProduct, &majorProductIsVariable);

		// find minor allele
		char minorAllele = 0;
		int minorCount = 0;
		alleles[majorAllele] = 0;  // no longer consider major allele
		char isTriallelic = findMaxAllele(&minorAllele, &minorCount);

		// find minor product
		char minorProduct = 63;  // 63 = ascii(A) - 1 which will be converted to '-' hsssFindMajorAlleles.c
		char minorProductIsVariable = 0;
		findMaxProduct(minorAllele, &minorProduct, &minorProductIsVariable);

		// write it out if has enough polymorphisms
		int16_t majorAllelePerTenThou = majorCount * 10000 / alleleCount;

		if (majorAllelePerTenThou >= minMajorAllelePerTenThou) {
			// calculate  major allele perTenThou and  minor allele perTenThou
			int16_t minorAllelePerTenThou = minorCount * 10000 / alleleCount;
			writeRecord(prevSeq, prevLoc, majorAllele, majorProduct, majorProductIsVariable, minorAllele, minorProduct, minorProductIsVariable, majorAllelePerTenThou, minorAllelePerTenThou, isTriallelic);
		}
	} 
	else {
		getRefGenomeInfo(refGenomeFileName, prevSeq, prevLoc);
	}

	// zero out counts
	int j; for (j=0; j<5; j++) alleles[j] = 0;
	nonRefStrainsCount = 0;
	alleleCount = 0;
	initProductArrays(products);

	// reset prev for new snp
	prevProduct = -1;
	prevStrain = -1;
}


