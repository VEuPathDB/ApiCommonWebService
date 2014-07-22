#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

static char *stdoutStr = "STDOUT";

static inline int fwriteCheck (char *filename, void *ptr, size_t size, size_t count, FILE *stream) {
	fwrite(ptr, size, count,stream);
	if (ferror(stream)) {
		fprintf(stderr, "Failed writing to stdout\n");
	 	exit(-1);
	}
}

inline static int readStrainRow(int16_t *seq, int32_t *loc, int8_t *allele, char *product) {
	fread(seq, 2, 1, stdin);  
	fread(loc, 4, 1, stdin);  
	fread(allele, 1, 1, stdin); 
	int retval = fread(product, 1, 1, stdin);
	return retval;
}

inline static int writeStrainRow(int16_t *seq_p, int32_t *loc_p, int8_t *allele, char *product, int16_t *strain) {
	fwriteCheck(stdoutStr, seq_p, 2, 1, stdout);  
	fwriteCheck(stdoutStr, loc_p, 4, 1, stdout);  
	fwriteCheck(stdoutStr, allele, 1, 1, stdout); 
	fwriteCheck(stdoutStr, product, 1, 1, stdout);
	fwriteCheck(stdoutStr, strain, 2, 1, stdout);
}

main(int argc, char *argv[]) {
	int16_t seq1;
  int16_t *seq1_p = &seq1;
  int32_t loc1;
  int32_t *loc1_p = &loc1;
  char a1;  // allele
  char *a1_p = &a1;
  char p1;  // product
  char *p1_p = &p1;
	int16_t cmdLineStrain1;

	if ( argc != 2 ) {
		printf( "usage: %s strain_id < strainFile\n\nAdd a strain ID column to a strain file. Writes to stdout.\n\n", argv[0] );
		return -1;
	}
	cmdLineStrain1 = atoi(argv[1]);

	int f1got;
	f1got = readStrainRow(seq1_p, loc1_p, a1_p, p1_p);

	while(f1got != 0) {
		writeStrainRow(seq1_p, loc1_p, a1_p, p1_p, &cmdLineStrain1);
		f1got = readStrainRow(seq1_p, loc1_p, a1_p, p1_p);
	}
	return 0;
}

