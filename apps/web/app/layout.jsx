import "./globals.css";
export const metadata = {
  title: "Premier League Analyst · 2022–2025",
  description: "Phân tích cầu thủ, phong độ CLB, chuyển nhượng, RFM/GMM và thống kê trận",
};
export default function Layout({ children }) {
  return (
    <html lang="vi">
      <body>{children}</body>
    </html>
  );
}
