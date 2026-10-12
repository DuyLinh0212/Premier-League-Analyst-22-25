"""Run with native Playwright against an already started web/API/Kafka stack."""
import json
import os
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

def main():
    evidence = Path(__file__).parent / 'evidence'
    evidence.mkdir(exist_ok=True)
    checks = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 1080})
        errors = []
        page.on('pageerror', lambda e: errors.append(str(e)))
        page.goto(os.getenv('WEB_URL', 'http://127.0.0.1:3000'))
        page.wait_for_load_state('networkidle')
        expect(page.get_by_role('heading', name='Một trận. Ba khả năng.')).to_be_visible()
        expect(page.locator('#home-possession')).to_have_value('50')
        assert page.locator('#home-possession').bounding_box()['width'] >= 55
        checks.append('desktop_home_and_away_inputs_visible')
        expect(page.locator('.f-evaluation > p > strong')).to_have_text('Random Forest')
        page.get_by_text('So sánh KNN, Logistic Regression và Random Forest',exact=True).click()
        expect(page.locator('.f-evaluation details').first.locator('tbody tr')).to_have_count(13)
        expect(page.locator('.f-evaluation details').first).to_contain_text('mùa kiểm thử không dùng để chọn model')
        page.get_by_text('So sánh KNN, Logistic Regression và Random Forest',exact=True).click()
        checks.append('selected_algorithm_and_validation_comparison_visible')
        page.get_by_role('button', name='Ước tính kết quả →').click()
        expect(page.locator('.f-model-id')).to_contain_text('wdl_', timeout=30000)
        expect(page.get_by_text('Đã điền', exact=False).first).to_be_visible()
        checks.append('real_api_prediction_with_missing_fields')
        page.screenshot(path=str(evidence / 'web_prediction.png'), full_page=True)
        page.locator('#home-shots_on_target').fill('99')
        page.get_by_role('button', name='Ước tính kết quả →').click()
        expect(page.locator('.f-error[role=alert]')).to_contain_text('không được vượt')
        checks.append('invalid_shots_rejected')
        page.locator('.f-sample select').select_option(index=1)
        expect(page.locator('#home-possession')).not_to_have_value('')
        checks.append('historical_sample_loads_both_sides')
        page.route('**/api/football/predict', lambda route: route.fulfill(status=503, json={'detail':'Dịch vụ phân tích chưa sẵn sàng.'}))
        page.get_by_role('button', name='Ước tính kết quả →').click()
        expect(page.locator('.f-error[role=alert]')).to_contain_text('chưa sẵn sàng')
        checks.append('service_failure_shown')
        page.unroute('**/api/football/predict')
        page.set_viewport_size({'width':390,'height':844})
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert page.locator('#home-possession').bounding_box()['width'] >= 55
        page.screenshot(path=str(evidence / 'web_mobile.png'), full_page=True)
        checks.append('mobile_no_horizontal_overflow')
        assert not errors, errors
        browser.close()
    report = {'passed':checks,'javascript_errors':errors}
    (evidence / 'web_tests.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))

if __name__ == '__main__':
    main()
