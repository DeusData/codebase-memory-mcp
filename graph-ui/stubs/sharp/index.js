'use strict';
// Stand-in for sharp (see package.json). Only the Node image path of
// @huggingface/transformers would call it; CBM Lens runs Transformers in the
// browser and never processes images, so a call here is a bug, not a fallback.
module.exports = function sharp() {
    throw new Error('sharp is not part of CBM Lens: image processing is unsupported');
};
