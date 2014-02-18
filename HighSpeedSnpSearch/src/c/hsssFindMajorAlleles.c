#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
	int16_t seq;
	int32_t loc;
	char majorAllele;  
	char majorProduct;  
	char majorProductIsVariable;  
	char minorAllele;  
	char minorProduct;  
	char minorProductIsVariable;  
	int16_t majorAllelePerTenThou;
	int16_t minorAllelePerTenThou;
	char isTriallelic;
} snp;

static char *stdoutStr = "STDOUT";

static char alleles[5] = {0, 'A', 'C', 'G', 'T'};
static char products[27] = {'-', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};

static inline int freadCheck (char *filename, void *ptr, size_t size, size_t count, FILE *stream) {
	int bytes = fread(ptr, size, count,stream);
	if (ferror(stream)) {
		fprintf(stderr, "Failed reading file '%s' \n", filename );
	 	exit(-1);
	}
	return bytes;
}

static inline int readSnp(FILE *stream, snp *snp, char *filename) {
	int16_t tmp;

	freadCheck(filename, &(*snp).seq, 2, 1, stream);
	freadCheck(filename, &(*snp).loc, 4, 1, stream);
	freadCheck(filename, &(*snp).majorAllele, 1, 1, stream);
	freadCheck(filename, &(*snp).majorProduct, 1, 1, stream);
	freadCheck(filename, &(*snp).majorProductIsVariable, 1, 1, stream);
	freadCheck(filename, &(*snp).minorAllele, 1, 1, stream);
	freadCheck(filename, &(*snp).minorProduct, 1, 1, stream);
	freadCheck(filename, &(*snp).minorProductIsVariable, 1, 1, stream);
	freadCheck(filename, &(*snp).majorAllelePerTenThou, 2, 1, stream);
	freadCheck(filename, &(*snp).minorAllelePerTenThou, 2, 1, stream);
	return freadCheck(filename, &(*snp).isTriallelic, 1, 1, stream);
}

static inline void reportSnp(snp snp1, snp snp2) {
	printf("%i\t%i\t%c\t%.2f\t%i\t%c\t%i\t%c\t%.2f\t%i\t%c\t%i\n", snp1.seq, snp1.loc, alleles[snp1.majorAllele], (float)snp1.majorAllelePerTenThou/100, snp1.isTriallelic, products[snp1.majorProduct-63], snp1.majorProductIsVariable, alleles[snp2.majorAllele], (float)snp2.majorAllelePerTenThou/100, snp2.isTriallelic, products[snp2.majorProduct-63], snp2.majorProductIsVariable);
}

main(int argc, char *argv[]) {
	FILE *f1;
	FILE *f2;
	snp snp1;
	snp snp2;

	if ( argc != 3 ) {
		fprintf(stderr, "usage: %s consensus_file1 consensus_file2\n\n", argv[0] );
		return -1;
	}

	f1 = fopen(argv[1], "rb");
	if (f1 == 0) {
		fprintf(stderr, "Can't open file1 '%s' \n", argv[1] );
		return -1;
	}

	f2 = fopen(argv[2], "rb");
	if (f2 == 0) {
		fprintf(stderr,"Can't open file2 '%s' \n", argv[2] );
		return -1;
	}

	int f1got;
	int f2got;
	f1got = readSnp(f1, &snp1, argv[1]);
	f2got = readSnp(f2, &snp2, argv[2]);

	while(f1got != 0 && f2got != 0) {
		// skip snps in file 1 absent from file 2
		while ((snp1.seq < snp2.seq || (snp1.seq == snp2.seq && snp1.loc < snp2.loc)) && f1got != 0) { 
			f1got = readSnp(f1, &snp1, argv[1]);
		}
		// skip snps in file 2 absent from file 1
		while ((snp2.seq < snp1.seq || (snp2.seq == snp1.seq && snp2.loc < snp1.loc)) && f2got != 0) {
			f2got = readSnp(f2, &snp2, argv[2]);
		}
		if (snp1.seq == snp2.seq && snp1.loc == snp2.loc) {
			if (snp1.majorAllele != snp2.majorAllele) reportSnp(snp1, snp2);
			f1got = readSnp(f1, &snp1, argv[1]);
			f2got = readSnp(f2, &snp2, argv[2]);
		}
	}
	fclose(f1);
	fclose(f2);
	return 0;
}


