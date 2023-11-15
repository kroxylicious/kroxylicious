use std::mem;
use std::slice;

#[no_mangle]
pub extern "C" fn alloc(len: i32) -> *const u8 {
    let mut buf = Vec::with_capacity(len as usize);
    let ptr = buf.as_mut_ptr();
    // tell Rust not to clean this up
    mem::forget(buf);
    ptr
}

#[no_mangle]
pub unsafe extern "C" fn dealloc(ptr: &mut u8, len: i32) {
    let _ = Vec::from_raw_parts(ptr, 0, len as usize);
}

#[no_mangle]
pub extern fn replace(ptr: i32, len: i32) -> i32 {
    let bytes = unsafe { slice::from_raw_parts_mut(ptr as *mut u8, len as usize) };
    let s = String::from_utf8(bytes.to_vec()).unwrap();
    let replaced = s.replace("bar", "baz").into_bytes();

    let mut idx: i32 = 0;
    for ch in replaced {
        bytes[idx as usize] = ch;
        idx += 1;
    }

    idx
}
