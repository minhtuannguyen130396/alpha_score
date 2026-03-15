PHẦN 2 — Input Architecture của Layer 2 theo Phase
2.1. Mục tiêu của phần này

Thay vì chỉ liệt kê “daily / bid-ask / tick”, PHẦN 2 nên mô tả input architecture theo các phase đi vào Layer 2.
Mỗi phase cần trả lời 3 câu hỏi:

nhận đầu vào gì

xử lý ở mức input như thế nào

xuất ra object nào để phase sau dùng tiếp

Cách viết này phù hợp hơn với tài liệu gốc, vì file architecture for layer 2.md đang yêu cầu tách rõ input, pipeline xử lý và output schema, đồng thời Layer 2 phải deterministic, không dùng dữ liệu tương lai và chỉ làm nhiệm vụ data refinery.

2.2. Tổng quan các phase trong PHẦN 2
Phase	Tên phase	Vai trò	Output chính
Phase 0	Raw source intake	Nạp 4 nguồn dữ liệu thực tế	raw_source_bundle
Phase 1	Schema normalization	Chuẩn hóa 4 nguồn về schema nội bộ thống nhất	normalized_input_bundle
Phase 2	Input validation & consistency check	Kiểm tra rule từng nguồn và liên hệ chéo giữa nguồn	validated_input_bundle
Phase 3	Time alignment & reference indexing	Căn chỉnh thời gian và dựng input theo mốc tham chiếu	aligned_input_frame
Phase 4	Book memory & deep reconstruction input	Dựng latent depth, wall memory, fake-wall candidates	deep_book_input_bundle
Phase 5	Layer-2 ready input package	Đóng gói toàn bộ input cuối để sang pipeline nội bộ	layer2_input_package