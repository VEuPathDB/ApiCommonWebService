#include <stdio.h>
#include <stdint.h>
#include <string.h>

inline static int readStrainRow(int16_t *seq, int32_t *loc, int8_t *allele, char *product) {
	fread(seq, 2, 1, stdin);  
	fread(loc, 4, 1, stdin);  
	fread(allele, 1, 1, stdin); 
	int retval = fread(product, 1, 1, stdin);
	return retval;
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

	int f1got;
	f1got = readStrainRow(seq1_p, loc1_p, a1_p, p1_p);

	while(f1got != 0) {
		printf("%i\t%i\t%i\t%i\n", seq1, loc1, a1, p1);
		f1got = readStrainRow(seq1_p, loc1_p, a1_p, p1_p);
	}
	return 0;
}

