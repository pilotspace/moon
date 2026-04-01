// Batch Randomized Fast Walsh-Hadamard Transform — CUDA kernel template.
//
// This kernel applies the randomized FWHT to a batch of vectors in parallel.
// Each thread block processes one vector using shared memory for the butterfly
// pattern. Sign flips are applied element-wise before the transform.
//
// STATUS: Template only — not compiled by build.rs yet.
//
// Compilation will be wired up when cudarc kernel loading is integrated.
// Expected invocation from Rust:
//   ctx.device().load_ptx(ptx, "turbo_quant_wht", &["batch_randomized_fwht"])?;
//   let func = ctx.device().get_func("turbo_quant_wht", "batch_randomized_fwht")?;

extern "C" __global__ void batch_randomized_fwht(
    float* __restrict__ vectors,      // [batch_size * padded_dim]
    const float* __restrict__ flips,   // [padded_dim] — sign flips (+1 or -1)
    const int padded_dim               // must be power of 2
) {
    // Each block processes one vector.
    // blockIdx.x = vector index within the batch.
    // threadIdx.x = element index within the vector (0..padded_dim/2).

    extern __shared__ float sdata[];

    const int vec_offset = blockIdx.x * padded_dim;
    const int tid = threadIdx.x;
    const int half_dim = padded_dim / 2;

    // Step 1: Load vector into shared memory and apply sign flips.
    if (tid < half_dim) {
        sdata[tid]            = vectors[vec_offset + tid]            * flips[tid];
        sdata[tid + half_dim] = vectors[vec_offset + tid + half_dim] * flips[tid + half_dim];
    }
    __syncthreads();

    // Step 2: Butterfly passes — log2(padded_dim) stages.
    for (int h = 1; h < padded_dim; h <<= 1) {
        // Each thread handles one butterfly pair.
        const int block_start = (tid / h) * (h * 2);
        const int offset = tid % h;
        const int i = block_start + offset;
        const int j = i + h;

        if (j < padded_dim) {
            float x = sdata[i];
            float y = sdata[j];
            sdata[i] = x + y;
            sdata[j] = x - y;
        }
        __syncthreads();
    }

    // Step 3: Normalize by 1/sqrt(padded_dim) and write back.
    const float norm = rsqrtf((float)padded_dim);
    if (tid < half_dim) {
        vectors[vec_offset + tid]            = sdata[tid]            * norm;
        vectors[vec_offset + tid + half_dim] = sdata[tid + half_dim] * norm;
    }
}
