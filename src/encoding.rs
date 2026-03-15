/// Encode p_brand byte string to a u8 index.
/// 0 = Brand#12, 1 = Brand#23, 2 = Brand#34, u8::MAX = no match.
pub fn brand_to_idx(bytes: &[u8]) -> u8 {
    match bytes {
        b"Brand#12" => 0,
        b"Brand#23" => 1,
        b"Brand#34" => 2,
        _ => u8::MAX,
    }
}

/// Encode p_container byte string to a u8 index.
/// Grouped: SM = 0..3, MED = 4..7, LG = 8..11, u8::MAX = no match.
pub fn container_to_idx(bytes: &[u8]) -> u8 {
    match bytes {
        // SM group (0..3)
        b"SM CASE" => 0,
        b"SM BOX" => 1,
        b"SM PACK" => 2,
        b"SM PKG" => 3,
        // MED group (4..7)
        b"MED BAG" => 4,
        b"MED BOX" => 5,
        b"MED PKG" => 6,
        b"MED PACK" => 7,
        // LG group (8..11)
        b"LG CASE" => 8,
        b"LG BOX" => 9,
        b"LG PACK" => 10,
        b"LG PKG" => 11,
        _ => u8::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_brand_encoding() {
        assert_eq!(brand_to_idx(b"Brand#12"), 0);
        assert_eq!(brand_to_idx(b"Brand#23"), 1);
        assert_eq!(brand_to_idx(b"Brand#34"), 2);
        assert_eq!(brand_to_idx(b"Brand#99"), u8::MAX);
        assert_eq!(brand_to_idx(b""), u8::MAX);
    }

    #[test]
    fn test_container_encoding_sm() {
        assert_eq!(container_to_idx(b"SM CASE"), 0);
        assert_eq!(container_to_idx(b"SM BOX"), 1);
        assert_eq!(container_to_idx(b"SM PACK"), 2);
        assert_eq!(container_to_idx(b"SM PKG"), 3);
    }

    #[test]
    fn test_container_encoding_med() {
        assert_eq!(container_to_idx(b"MED BAG"), 4);
        assert_eq!(container_to_idx(b"MED BOX"), 5);
        assert_eq!(container_to_idx(b"MED PKG"), 6);
        assert_eq!(container_to_idx(b"MED PACK"), 7);
    }

    #[test]
    fn test_container_encoding_lg() {
        assert_eq!(container_to_idx(b"LG CASE"), 8);
        assert_eq!(container_to_idx(b"LG BOX"), 9);
        assert_eq!(container_to_idx(b"LG PACK"), 10);
        assert_eq!(container_to_idx(b"LG PKG"), 11);
    }

    #[test]
    fn test_container_encoding_invalid() {
        assert_eq!(container_to_idx(b"XL BOX"), u8::MAX);
        assert_eq!(container_to_idx(b""), u8::MAX);
    }
}
