import streamlit as st
import os
from web3 import Web3
import time
import pandas as pd
import requests
from datetime import datetime

# Cấu hình trang
st.set_page_config(
    page_title="Metamask Connect App",
    page_icon="🦊",
    layout="wide"
)

# Kết nối với Web3
w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/YOUR_INFURA_KEY'))

# Khởi tạo session state
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'address' not in st.session_state:
    st.session_state.address = None
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'posts' not in st.session_state:
    # Giả lập dữ liệu API
    st.session_state.posts = []

# Load CSS từ file styles.css
def load_css():
    with open("src/services/streamlit/style.css", "r") as f:
        css = f.read()
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)

load_css()

# Hàm JavaScript để kết nối với Metamask
connect_script = """
<script>
async function connectWallet() {
    if (typeof window.ethereum !== 'undefined') {
        try {
            const accounts = await window.ethereum.request({ method: 'eth_requestAccounts' });
            const account = accounts[0];
            
            // Gửi địa chỉ ví đến Streamlit
            const data = {
                address: account,
                connected: true
            };
            
            // Gửi dữ liệu thông qua streamlit-component-lib
            window.parent.postMessage({
                type: "streamlit:setComponentValue",
                value: JSON.stringify(data)
            }, "*");
            
        } catch (error) {
            console.error(error);
        }
    } else {
        alert('Metamask không được cài đặt. Vui lòng cài đặt Metamask extension.');
    }
}
</script>

<button onclick="connectWallet()" style="
    background-color: #FF9E0D;
    color: white;
    border: none;
    border-radius: 5px;
    padding: 10px 20px;
    cursor: pointer;
    font-weight: bold;
">Kết nối với Metamask</button>
"""

# Hàm kiểm tra kết nối ví
def connect_wallet():
    st.components.v1.html(connect_script, height=80)
    
    # Đây là hack để đọc giá trị từ JavaScript
    component_value = st.empty()
    component_value = st.components.v1.html(
        """
        <div id="wallet_data"></div>
        <script>
        window.addEventListener('message', function(e) {
            if (e.data.type === 'streamlit:setComponentValue') {
                document.getElementById('wallet_data').innerText = e.data.value;
            }
        });
        </script>
        """,
        height=0,
    )

# Hàm để lấy dữ liệu từ API
def fetch_posts_from_api():
    """
    Hàm này sẽ lấy dữ liệu từ API của bạn
    Thay thế bằng URL API thực tế của bạn
    """
    try:
        # Thay thế URL này bằng API của bạn
        # response = requests.get('https://your-api-endpoint.com/posts')
        # return response.json()
        
        # Dữ liệu mẫu giả lập API
        return [
            {
                "id": "1",
                "author": "0x123...abc",
                "content": "Chào mừng đến với ứng dụng Web3 Social!",
                "timestamp": "2025-03-13 10:00:00",
                "likes": 15,
                "comments": 3
            },
            {
                "id": "2",
                "author": "0x456...def",
                "content": "Web3 là tương lai của internet. Blockchain mang lại sự minh bạch và phi tập trung cho các ứng dụng và dịch vụ trực tuyến.",
                "timestamp": "2025-03-13 09:30:00",
                "likes": 27,
                "comments": 8
            },
            {
                "id": "3", 
                "author": "0x789...ghi",
                "content": "NFT và DeFi đang thay đổi cách chúng ta tương tác với tài sản kỹ thuật số và tài chính.",
                "timestamp": "2025-03-13 08:45:00",
                "likes": 42,
                "comments": 12
            },
            {
                "id": "4",
                "author": "0xabc...123",
                "content": "Hôm nay giá BTC đang tăng mạnh! Cộng đồng crypto rất hào hứng với sự phát triển này.",
                "timestamp": "2025-03-13 07:15:00",
                "likes": 38,
                "comments": 15
            },
            {
                "id": "5",
                "author": "0xdef...456",
                "content": "Đang nghiên cứu về các giải pháp Layer 2 để tăng khả năng mở rộng cho Ethereum. Có ai có kinh nghiệm với Optimism hoặc Arbitrum không?",
                "timestamp": "2025-03-12 23:10:00",
                "likes": 19,
                "comments": 7
            }
        ]
    except Exception as e:
        st.error(f"Lỗi khi lấy dữ liệu từ API: {e}")
        return []

# Container chính căn giữa
st.markdown('<div class="center-container">', unsafe_allow_html=True)

# Hiển thị header chính
st.markdown('<h1 class="main-header">Ứng dụng Web3 Social</h1>', unsafe_allow_html=True)

# Phần kết nối ví (căn giữa)
# Phần kết nối ví (được giữ lại sau khi load lại)
if not st.session_state.connected:
    st.markdown('<div style="text-align: center; margin: 20px 0;">', unsafe_allow_html=True)
    
    # Nút của Streamlit
    if st.button("🦊 Kết nối với Metamask", key="connect_button"):
        st.session_state.connected = True
        st.session_state.address = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
        st.rerun()
    
    st.markdown('</div>', unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div style="text-align: center; margin: 20px 0;">
        <p>Đã kết nối với ví: <strong>{st.session_state.address[:6]}...{st.session_state.address[-4:]}</strong></p>
        <button style="margin-top: 10px; background-color: #f44336; color: white; border: none; border-radius: 5px; padding: 5px 10px; cursor: pointer;">Ngắt kết nối</button>
    </div>
    """, unsafe_allow_html=True)
    
    # Tabs cho Feed và Chat
    tab1, tab2 = st.tabs(["📱 Feed", "💬 Chat"])
    
    # Tab Feed
    with tab1:
        st.subheader("Feed")
        
        # Lấy dữ liệu từ API
        posts = fetch_posts_from_api()
        
        # Hiển thị nút làm mới
        col1, col2, col3 = st.columns([2, 1, 2])
        with col2:
            if st.button("🔄 Làm mới Feed"):
                st.rerun()

        # Bắt đầu container cuộn cho Feed
        st.markdown('<div class="feed-container">', unsafe_allow_html=True)
        
        if not posts:
            st.info("Không có bài đăng nào. Vui lòng thử lại sau.")
        else:
            for post in posts:
                st.markdown(f"""
                <div class="post">
                    <div class="post-header">
                        <span class="post-author">{post["author"]}</span>
                        <span class="post-time">{post["timestamp"]}</span>
                    </div>
                    <div class="post-content">{post["content"]}</div>
                    <div class="post-actions">
                        <span>❤️ {post["likes"]} thích</span>
                        <span>💬 {post["comments"]} bình luận</span>
                        <span>🔁 Chia sẻ</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        # Kết thúc container cuộn
        st.markdown('</div>', unsafe_allow_html=True)

    # Tab Chat
    with tab2:
        st.subheader("Chat")

        # BẮT ĐẦU khung Chat
        st.markdown('<div class="chat-container-fixed">', unsafe_allow_html=True)
        
        # Hiển thị các tin nhắn
        for msg in st.session_state.messages:
            if msg["sender"] == st.session_state.address:
                st.markdown(f"""
                <div class="message message-sent">
                    <div><strong>Bạn</strong></div>
                    <div>{msg["content"]}</div>
                    <div style="text-align: right; font-size: 0.8rem; color: #606060;">{msg["timestamp"]}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="message message-received">
                    <div><strong>{msg["sender"][:6]}...{msg["sender"][-4:]}</strong></div>
                    <div>{msg["content"]}</div>
                    <div style="text-align: right; font-size: 0.8rem; color: #606060;">{msg["timestamp"]}</div>
                </div>
                """, unsafe_allow_html=True)
        # KẾT THÚC khung Chat
        st.markdown('</div>', unsafe_allow_html=True)
            
        with st.form(key="message_form"):
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            col1, col2 = st.columns([9, 1])  # Điều chỉnh tỷ lệ giữa input và nút gửi
            
            with col1:
                message_content = st.text_input("", placeholder="", key="message_input")

            with col2:
                st.markdown('<div class="send-btn">', unsafe_allow_html=True)
                send_button = st.form_submit_button("📨 Gửi")
                st.markdown('</div>', unsafe_allow_html=True)

            st.markdown('</div>', unsafe_allow_html=True)

            if send_button and message_content:
                new_message = {
                    "sender": st.session_state.address,
                    "content": message_content,
                    "timestamp": datetime.now().strftime("%H:%M")
                }
                st.session_state.messages.append(new_message)

                # Giả lập phản hồi
                if len(st.session_state.messages) % 2 == 1:
                    time.sleep(1)  # Giả lập độ trễ
                    response_message = {
                        "sender": "0x742d35Cc6634C0532925a3b844Bc454e4438abcd",
                        "content": "Cảm ơn bạn đã nhắn tin! Đây là tin nhắn tự động.",
                        "timestamp": datetime.now().strftime("%H:%M")
                    }
                    st.session_state.messages.append(response_message)

                st.rerun()


# Footer (căn giữa)
st.markdown("""
<div class="footer">
    <hr>
    <p>© 2025 Ứng dụng Web3 Social | Powered by Streamlit</p>
</div>
""", unsafe_allow_html=True)

# Đóng container chính
st.markdown('</div>', unsafe_allow_html=True)