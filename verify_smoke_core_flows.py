import io
import os
import tempfile
from datetime import datetime


tmp_dir = tempfile.mkdtemp(prefix='money_manager_smoke_')
db_path = os.path.join(tmp_dir, 'smoke.db').replace('\\', '/')
os.environ['DATABASE_URL'] = f'sqlite:///{db_path}'
os.environ['SECRET_KEY'] = 'smoke-test-secret'

from app import app, db, seed_defaults, Account, Category, Transaction, User  # noqa: E402


def fail(message: str) -> None:
    raise AssertionError(message)


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


with app.app_context():
    db.drop_all()
    db.create_all()
    seed_defaults()

client = app.test_client()
test_email = 'smoke.user@example.com'
test_password = 'StrongPass123!'

# 1) Signup
signup_response = client.post(
    '/signup',
    data={
        'name': 'Smoke User',
        'email': test_email,
        'password': test_password,
        'confirm_password': test_password,
    },
    follow_redirects=True,
)
assert_true(signup_response.status_code == 200, 'Signup request failed.')

with app.app_context():
    signed_up_user = User.query.filter_by(email=test_email).first()
    assert_true(signed_up_user is not None, 'Signup did not create user.')

# 2) Logout + login
client.get('/logout', follow_redirects=True)
login_response = client.post(
    '/login',
    data={'email': test_email, 'password': test_password},
    follow_redirects=True,
)
assert_true(login_response.status_code == 200, 'Login request failed.')
with client.session_transaction() as session_data:
    assert_true(bool(session_data.get('user_id')), 'Login did not set session user_id.')

# 3) Add category
category_name = 'Smoke Test Category'
category_response = client.post(
    '/categories/add',
    data={
        'name': category_name,
        'kind': 'expense',
        'icon': 'bi bi-cart',
        'color': '#0d6efd',
        'is_active': 'on',
    },
    follow_redirects=True,
)
assert_true(category_response.status_code == 200, 'Add category request failed.')

with app.app_context():
    custom_category = Category.query.filter_by(name=category_name).first()
    assert_true(custom_category is not None, 'Category was not inserted.')
    account = Account.query.order_by(Account.id).first()
    assert_true(account is not None, 'No account available for transaction test.')

# 4) Add expense transaction
tx_response = client.post(
    '/transactions/add',
    data={
        'tx_date': datetime.utcnow().strftime('%Y-%m-%dT%H:%M'),
        'description': 'Smoke Coffee',
        'amount': '199.00',
        'tx_type': 'expense',
        'notes': 'Smoke test expense',
        'payee': 'Cafe',
        'reference_no': 'SMOKE-REF-1',
        'account_id': str(account.id),
        'category_id': str(custom_category.id),
    },
    follow_redirects=True,
)
assert_true(tx_response.status_code == 200, 'Add transaction request failed.')

# 5) Import CSV
csv_content = (
    'Date,Description,Amount,Type,Category,Reference Number\n'
    '2026-03-01,Imported Tea,120.50,expense,Groceries,SMOKE-CSV-1\n'
)
import_response = client.post(
    '/import',
    data={
        'statement_file': (io.BytesIO(csv_content.encode('utf-8')), 'smoke.csv'),
        'account_id': str(account.id),
        'date_col': 'Date',
        'desc_col': 'Description',
        'amount_col': 'Amount',
        'type_col': 'Type',
        'category_col': 'Category',
        'do_import': '1',
    },
    content_type='multipart/form-data',
    follow_redirects=True,
)
assert_true(import_response.status_code == 200, 'Import request failed.')

with app.app_context():
    category_exists = Category.query.filter_by(name=category_name).count()
    tx_count = Transaction.query.count()
    imported_tx = Transaction.query.filter_by(reference_no='SMOKE-CSV-1').first()
    assert_true(category_exists == 1, 'Custom category count mismatch.')
    assert_true(tx_count >= 2, f'Expected at least 2 transactions, found {tx_count}.')
    assert_true(imported_tx is not None, 'Imported CSV transaction not found.')

print('SMOKE TEST PASSED: login + add expense + category + import flow is working.')
